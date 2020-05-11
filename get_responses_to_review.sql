drop view if exists latest_response_by_discord;
create view latest_response_by_discord as
with latest_response_id as (
    select "Team Captain Discord", max(id) as id
    from response
    group by "Team Captain Discord")
select response.*
from response
         join latest_response_id
              on response."Team Captain Discord" = latest_response_id."Team Captain Discord"
                  and response.id = latest_response_id.id;

drop view if exists latest_response;
create view latest_response as
with latest_response_id_by_btag as (
    select "Team Captain Battletag ", max(id) as id
    from latest_response_by_discord
    group by "Team Captain Battletag ")
select latest_response_by_discord.*
from latest_response_by_discord
         join latest_response_id_by_btag
              on latest_response_by_discord."Team Captain Battletag " = latest_response_id_by_btag."Team Captain Battletag "
                  and latest_response_by_discord.id = latest_response_id_by_btag.id;

drop view if exists id_battletag;
create view id_battletag as
select distinct latest_response.id,
                b.battletag
From latest_response
         join battletag b on latest_response.id = b.response_id;

drop view if exists duplicate_battletag;
create view duplicate_battletag as
select battletag, group_concat(id)
from id_battletag
group by battletag
having count(*) > 1;

drop view if exists response_with_duplicate_battletag;
create view if not exists response_with_duplicate_battletag as
select distinct id
from duplicate_battletag
         join id_battletag
              on duplicate_battletag.battletag = id_battletag.battletag;

select response.*
from response
         join response_with_duplicate_battletag on response.id = response_with_duplicate_battletag.id;