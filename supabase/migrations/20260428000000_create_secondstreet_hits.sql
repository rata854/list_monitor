create table if not exists secondstreet_hits (
  id          bigint generated always as identity primary key,
  date        date         not null,
  asin        text         not null,
  url         text         not null,
  price       integer      not null,
  fee         integer      not null default 770,
  image       text,
  title       text,
  description text,
  created_at  timestamptz  default now(),
  unique (asin, url)
);
