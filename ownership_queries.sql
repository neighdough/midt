-- SF Residential Rental Counts
select trim(both ' ' from own1) own1, own_adr, count(par.parid) num_props
from 
(select o.parid, own1, concat(o.adrno, o.adrstr, o.zip1) adr,
    concat(o.adrno, ' ', o.adrstr, ' | ', o.cityname, ', ', o.statecode, ' | ', 
        o.zip1) own_adr
    from sca_owndat o) own,
(select parid,	concat(p.adrno,p.adrstr, p.zip1) par_adr, 
	luc from sca_pardat p where luc = '062') par
where adr <> par_adr and par.parid = own.parid
and lower(own1) similar to '%((city of )?memphis (city of)|(mlg\&?w)|(housing authority))%'
-- and lower(own1) similar to '%memphis%'
group by own1, own_adr
order by own1 desc -- num_props desc

--all code violators
select case
		when lower(own1) like 'shelby county tax sale%' 
        	then 'Shelby County Tax Sale' 
        when lower(concat(adrno,adrdir,adrstr,adrsuf)) = '125nmainst' 
        	then 'City of Memphis' 
		else trim(both ' ' from own1) end as own,
		par_adr, p.parcelid, reported_date, 
    request_type, summary, wkb_geometry 
from 
	(select parcelid, concat(adrno, ' ', adrstr, ' ', adrsuf, ', ', zip1) par_adr,
    		summary, reported_date, 
            split_part(request_type, '-', 2) request_type,
     		wkb_geometry 
    	from sca_pardat, sca_parcels, com_incident
    	where parcelid = parid 
    		and parcelid = parcel_id
    		and reported_date 
    			between '2018-01-01'::timestamp and '2018-01-31'::timestamp) p,
	sca_owndat
where parcelid = parid
order by own;

-- aggregates summary and request_type fields into single column
select own, count(own), array_to_string(array_agg(summary), ' | ') summary,
array_to_string(array_agg(request_type), '; ') req_type
from (select 
      	case when lower(own1) like 'shelby county tax sale%' then 'Shelby County Tax Sale'
      	when lower(concat(adrno,adrdir,adrstr,adrsuf)) = '125nmainst' then 'City of Memphis'
      	else trim(both ' ' from own1) end as own, 
      parid from sca_owndat) o,
(select parcel_id, summary, split_part(request_type, '-', 2) request_type from com_incident  
 	where reported_date between '2018-01-01'::timestamp and '2018-01-31'::timestamp) incid
where parcel_id = parid
group by own 
order by count desc limit 10

