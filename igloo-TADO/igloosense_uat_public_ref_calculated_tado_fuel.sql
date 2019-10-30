CREATE TABLE public.ref_calculated_tado_fuel
(
    fuel_id bigint,
    fuel_tariff_name varchar(255),
    fuel_region varchar(255),
    fuel_start_date timestamp,
    fuel_end_date timestamp,
    fuel_description varchar(255),
    fuel_rate double precision,
    fuel_coefficient double precision,
    fuel_calculation varchar(255)
);
INSERT INTO public.ref_calculated_tado_fuel (fuel_id, fuel_tariff_name, fuel_region, fuel_start_date, fuel_end_date, fuel_description, fuel_rate, fuel_coefficient, fuel_calculation) VALUES (2, 'gas', 'all', '2019-01-01 00:00:00.904000', null, 'gas off grid', 3.2, 1, null);
INSERT INTO public.ref_calculated_tado_fuel (fuel_id, fuel_tariff_name, fuel_region, fuel_start_date, fuel_end_date, fuel_description, fuel_rate, fuel_coefficient, fuel_calculation) VALUES (1, 'oil', 'all', '2019-01-01 00:00:00.904000', null, 'oil off grid', 4.83, 1, '');