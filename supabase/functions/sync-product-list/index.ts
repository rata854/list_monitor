import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "jsr:@supabase/supabase-js@2";

const SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly";

async function importPrivateKey(pem: string): Promise<CryptoKey> {
  const pemBody = pem
    .replace(/-----BEGIN PRIVATE KEY-----/g, "")
    .replace(/-----END PRIVATE KEY-----/g, "")
    .replace(/\\n/g, "")
    .replace(/\n/g, "")
    .replace(/\s/g, "")
    .trim();
  const binary = atob(pemBody);
  const buffer = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) buffer[i] = binary.charCodeAt(i);
  return crypto.subtle.importKey(
    "pkcs8",
    buffer.buffer,
    { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
    false,
    ["sign"]
  );
}

function base64url(data: string | Uint8Array): string {
  const str = typeof data === "string" ? btoa(data) : btoa(String.fromCharCode(...data));
  return str.replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
}

async function getAccessToken(email: string, privateKeyPem: string): Promise<string> {
  const key = await importPrivateKey(privateKeyPem);
  const now = Math.floor(Date.now() / 1000);
  const header  = base64url(JSON.stringify({ alg: "RS256", typ: "JWT" }));
  const payload = base64url(JSON.stringify({
    iss: email, scope: SHEETS_SCOPE,
    aud: "https://oauth2.googleapis.com/token",
    iat: now, exp: now + 3600,
  }));
  const signingInput = `${header}.${payload}`;
  const signature = await crypto.subtle.sign(
    "RSASSA-PKCS1-v1_5", key,
    new TextEncoder().encode(signingInput)
  );
  const jwt = `${signingInput}.${base64url(new Uint8Array(signature))}`;
  const res = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=${jwt}`,
  });
  const { access_token } = await res.json();
  return access_token;
}

async function readSheet(accessToken: string, spreadsheetId: string): Promise<string[][]> {
  const range = encodeURIComponent("商品リスト!A:CZ");
  const res = await fetch(
    `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}/values/${range}`,
    { headers: { Authorization: `Bearer ${accessToken}` } }
  );
  const data = await res.json();
  return data.values ?? [];
}

function parsePrice(v: string | undefined): number | null {
  if (v === undefined || v === null || v === "") return null;
  const n = parseFloat(String(v).replace(/,/g, ""));
  return isNaN(n) ? null : n;
}

Deno.serve(async (_req: Request) => {
  try {
    const email         = Deno.env.get("GOOGLE_SERVICE_ACCOUNT_EMAIL")!;
    const privateKey    = Deno.env.get("GOOGLE_PRIVATE_KEY")!;
    const spreadsheetId = Deno.env.get("SPREADSHEET_ID")!;

    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );

    const accessToken = await getAccessToken(email, privateKey);
    const rows = await readSheet(accessToken, spreadsheetId);
    if (rows.length === 0) throw new Error("シートが空です");

    const headers  = rows[0];
    const dataRows = rows.slice(4);
    const idx = (col: string) => headers.indexOf(col);

    console.log("headers:", JSON.stringify(headers));
    console.log("AUTO_FLAG idx:", idx("AUTO_FLAG"));
    console.log("first data row sample:", JSON.stringify(dataRows[0]));

    const filteredRows = dataRows.filter(row => row[idx("ASIN_SELL")]?.trim());

    const records = filteredRows
      .map(row => ({
        asin_sell:        row[idx("ASIN_SELL")],
        product_code_out: row[idx("PRODUCT_CODE_OUT")] || null,
        must_keywords:    row[idx("MUST_KEYWORDS")]    || null,
        final_price:      parsePrice(row[idx("FINAL_PRICE")]),
        auto_flag:        row[idx("AUTO_FLAG")]?.toString().toLowerCase() === "true",
        synced_at:        new Date().toISOString(),
      }));

    if (records.length === 0) {
      return new Response(JSON.stringify({ message: "同期対象なし" }), {
        headers: { "Content-Type": "application/json" },
      });
    }

    const { error } = await supabase
      .from("product_list")
      .upsert(records, { onConflict: "asin_sell" });
    if (error) throw error;

    return new Response(JSON.stringify({ synced: records.length }), {
      headers: { "Content-Type": "application/json" },
    });
  } catch (err) {
    return new Response(JSON.stringify({ error: String(err) }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }
});
