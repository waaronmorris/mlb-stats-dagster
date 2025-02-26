Select
{#    {{  dbt_utils.star(from=source("cloudflare", "raw_league_transactions"), except=["transaction_id", "date"], relation_alias="transactions") }}#}
    * EXCLUDE (date)
  , try_strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ') as date
FROM
    {{ source("cloudflare", "raw_league_transactions") }} transactions