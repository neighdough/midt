with bus as (select min(stnum)street_min, max(stnum) street_max, name street_name, 
	              disp_name district_name
             from geography.sc911_address l, geography.bp_districts d
					   where st_within(l.wkb_geometry, d.wkb_geometry)
			       group by name, disp_name)
select * from bus order by district_name
