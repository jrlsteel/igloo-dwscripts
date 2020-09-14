update ref_tariffs
set end_date='2020-10-14'
where id between 183 and 195
   or id between 209 and 221;

insert into ref_tariffs (id, fuel_type, gsp_ldz, name, billing_start_date, signup_start_date, end_date, standing_charge,
                         unit_rate, discounts, tariff_type, exit_fees)
VALUES (222, 'E', '_A', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 21.562, 13.57, null, 'variable', null),
       (223, 'G', '_A', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 24.833, 2.299, null, 'variable', null),
       (224, 'E', '_B', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 21.562, 13.21, null, 'variable', null),
       (225, 'G', '_B', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 24.833, 2.202, null, 'variable', null),
       (226, 'E', '_C', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 21.562, 13.107, null, 'variable', null),
       (227, 'G', '_C', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 24.833, 2.454, null, 'variable', null),
       (228, 'E', '_D', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 21.562, 14.446, null, 'variable', null),
       (229, 'G', '_D', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 24.833, 2.445, null, 'variable', null),
       (230, 'E', '_E', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 21.562, 13.948, null, 'variable', null),
       (231, 'G', '_E', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 24.833, 2.23, null, 'variable', null),
       (232, 'E', '_F', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 21.562, 13.608, null, 'variable', null),
       (233, 'G', '_F', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 24.833, 2.287, null, 'variable', null),
       (234, 'E', '_G', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 21.562, 13.807, null, 'variable', null),
       (235, 'G', '_G', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 24.833, 2.289, null, 'variable', null),
       (236, 'E', '_N', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 21.562, 13.735, null, 'variable', null),
       (237, 'G', '_N', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 24.833, 2.325, null, 'variable', null),
       (238, 'E', '_J', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 21.562, 14.083, null, 'variable', null),
       (239, 'G', '_J', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 24.833, 2.451, null, 'variable', null),
       (240, 'E', '_H', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 21.562, 13.757, null, 'variable', null),
       (241, 'G', '_H', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 24.833, 2.321, null, 'variable', null),
       (242, 'E', '_K', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 21.562, 14.192, null, 'variable', null),
       (243, 'G', '_K', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 24.833, 2.262, null, 'variable', null),
       (244, 'E', '_L', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 21.562, 14.807, null, 'variable', null),
       (245, 'G', '_L', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 24.833, 2.381, null, 'variable', null),
       (246, 'E', '_M', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 21.562, 13.395, null, 'variable', null),
       (247, 'G', '_M', 'Igloo Pioneer', '2020-10-15', '2020-09-15', null, 24.833, 2.264, null, 'variable', null);
