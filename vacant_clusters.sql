with merged as	
	(select (st_dump(st_union(wkb_geometry))).geom from austin.hom_vacant),
	all_ as (select (st_dump(wkb_geometry)).geom from austin.hom_vacant)
select count(*), merged.geom, st_area(merged.geom)/43560 acres
from merged, all_
where all_.geom && merged.geom and st_within(all_.geom, merged.geom) 
group by merged.geom
having count(*) > 1

