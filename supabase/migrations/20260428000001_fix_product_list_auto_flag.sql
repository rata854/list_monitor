alter table product_list
  alter column auto_flag type boolean
  using (auto_flag::text ilike 'true');
