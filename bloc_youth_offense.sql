select o.caseno, o.incident_date, o.dow, o.report_date, 
    o.address, o.ward, o.offense, o.dept_code, o.block, 
    o.street_address, o.ucr_code, o.weapon_code, 
    o.weapon_desc, o.case_status, o.jurisdiction, 
    case 
        when age <= 24 then 'Y' 
        else 'N' 
    end as youth, 
    extract (quarter from incident_date) quarter, 
    case  
        when st_intersects(o.wkb_geometry, b.wkb_geometry) 
    then name 
        else 'Memphis' 
    end focal_area 
from im_p1_violent_crime_offenses o 
left join 
    (select caseno, min(age) age 
        from im_p1_violent_crime_suspects group by caseno) s 
    on s.caseno = o.caseno 
left join 
    (select name, wkb_geometry 
        from geography.boundaries where origin = 'IM') b 
        on st_intersects(o.wkb_geometry, b.wkb_geometry) 