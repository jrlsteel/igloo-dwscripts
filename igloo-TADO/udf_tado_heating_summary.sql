-- UDF tado_heating_summary
create or replace function tado_heating_summary(custom_values varchar(10000)) returns varchar(500)
	stable
	language plpythonu
as
$$
    import json
    import logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    hs = 'unknown'
    if custom_values != '':
    		json_values = json.loads(custom_values)
    else:
    		return hs
    ages = []
    for d in json_values:
        if d['age'] == '0 to 9':
            ages.append(1)
        if d['age'] == '10 to 17':
            ages.append(2)
        if d['age'] == '18 to 64':
            ages.append(3)
        if d['age'] == '65 to 74':
            ages.append(4)
        if d['age'] == '75 and over':
            ages.append(5)

    no_oc = len(ages)

    if no_oc == 0:
        ft = 'unknown_no_occupants_reported'
        hs = 'unknown'
    else:
        if no_oc == 1:
            if ages[0] == 3:
                ft = 'working_age_individual'
                hs = 'working_no_kids'
            elif ages[0] > 3:
                ft = 'retired_individual'
                hs = 'retired'
            else:
                ft = 'unknown_bad_age_data'
                hs = 'unknown'

        elif no_oc == 2:
            if all([a == 3 for a in ages]):
                ft = 'working_age_couple'
                hs = 'working_no_kids'

            elif sum(ages) > 6:
                ft = 'retired_couple'
                hs = 'retired'

            elif sum(ages) < 6:
                if 1 in ages or 2 in ages and 3 in ages:
                    ft = 'single_parent'
                    hs = 'working_kids'

                elif 1 in ages or 2 in ages and 3 in ages or 4 in ages:
                    ft = 'retired_parent'
                    hs = 'retired'

                else:
                    ft = 'undefined_type_2'
                    hs = 'unknown'

        elif no_oc == 3:
            if all([a == 3 for a in ages]):
                ft = 'working_house_share_3'
                hs = 'working_no_kids'

            elif sum(ages) > 12:
                ft = 'retired_3'
                hs = 'retired'
            else:
                ft = 'working_age_family_3'
                hs = 'working_kids'

        elif no_oc == 4:
            if all([a == 3 for a in ages]):
                ft = 'working_house_share_4'
                hs = 'working_no_kids'

            elif sum(ages) > 16:
                ft = 'retired_4'
                hs = 'retired'

            else:
                ft = 'working_age_family_4'
                hs = 'working_kids'

        else:
            if all([a == 3 for a in ages]):
                ft = 'working_house_share_5plus'
                hs = 'working_no_kids'

            elif sum(ages) > 20:
                ft = 'retired_5plus'
                hs = 'retired'

            else:
                ft = 'working_age_family_5plus'
                hs = 'working_kids'

    return hs

$$
;
