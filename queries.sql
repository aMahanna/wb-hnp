-- Slice
-- Read yearly Life Satisfaction (measure) for each North American Country
SELECT DISTINCT
    C.name, F.life_satis, M.year  
FROM
    wb_hnp F, country C, month M
WHERE 
    F.country_key = C.country_key AND 
    region = 'North America' AND
    M.month_key = F.month_key
ORDER BY C.name, M.year

-- Dice #1
-- Read Country Name (attribute), Life Satisfaction (measure) & COVID-19 Deaths (attribute) 
-- for each Country that has more than 20000 COVID-19 deaths
SELECT DISTINCT
    C.name, F.life_satis, M.year, E.Deaths
FROM 
    wb_hnp F, event E, month M, country C
WHERE 
    E.deaths > 20000 AND E.name = 'COVID-19' AND
    M.month_key = F.month_key AND
    C.country_key = F.country_key AND
    E.event_key = F.event_key
ORDER BY M.year

-- Dice #2
-- Read Anemia & Overweight insight for Countries with specific Anemia & Overweight ratios
SELECT DISTINCT 
    C.name, M.year, N.anemia_preg, N.anemia_non_preg,
    N.overweight_female, N.overweight_male
FROM 
    nutrition N, country C, month M, wb_hnp F
WHERE 
    N.anemia_preg > N.anemia_non_preg AND
    N.overweight_female > N.overweight_male AND
    N.nutrition_key = F.nutrition_key AND
    C.country_key = F.country_key AND
    M.month_key = F.month_key
ORDER BY C.name, M.year


-- Roll Up (Country --> Region)
-- Happiest Regions
SELECT
    RESULT.region, RESULT.name, AVG(RESULT.life_satis) AS average_life_satis
FROM (
    SELECT DISTINCT C.name, C.region, F.life_satis, M.year 
    FROM country C, wb_hnp F, month M 
    WHERE C.country_key = F.country_key AND F.month_key = M.month_key
    ORDER BY C.name, M.year
) AS RESULT 
GROUP BY ROLLUP(RESULT.region, RESULT.name);


-- Drill down (Decade --> Month)
-- Monthly events activity from a 2010 decade perspective
SELECT
    E.name, E.deaths, M.name AS start_month, M.year AS start_year
FROM
    event E, month M
WHERE
    e.start_date = M.month_key AND M.decade = 2010
GROUP BY (E.name, E.deaths, M.name, M.year, M.month_key) 
ORDER BY M.year, M.month_key;


-- Iceberg
-- Canada's Top 5 out of 16 population growth years
SELECT DISTINCT
    country.name,
    population.p_growth_urban + population.p_growth_rural AS population_growth,
	month.year
FROM wb_hnp, population, country, month
WHERE 
	wb_hnp.country_key = country.country_key AND
	wb_hnp.population_key = population.population_key AND
	wb_hnp.month_key = month.month_key AND
	country.name = 'Canada'
ORDER BY population_growth DESC
LIMIT 5


-- Slice + Roll Up (Country --> Region)
-- Average HDI between 2010 - 2015
SELECT
    RESULT.region, 
    RESULT.name, 
    ROUND(CAST(AVG(RESULT.hdi) AS NUMERIC), 4) AS average_hdi
FROM (
    SELECT DISTINCT c.name, c.region, f.hdi, m.year
	FROM country c, wb_hnp f, month m 
	WHERE 
        c.country_key = f.country_key 
		AND f.month_key = m.month_key
		AND m.year BETWEEN 2010 AND 2015
	ORDER BY c.name, m.year
) AS RESULT
GROUP BY ROLLUP(RESULT.region, RESULT.name)

-- Dice + Drill Down (Decade --> Year)
-- Yearly GNI for Asian on years where population growth was positive in the 2010 decade
SELECT DISTINCT c.name, c.region, f.gni, m.year
FROM country c, wb_hnp f, month m 
WHERE
    c.country_key = f.country_key 
	AND f.month_key = m.month_key
	and m.decade = 2010
	and c.population_growth > 0 
	and c.region in ('East Asia & Pacific','South Asia','Middle East & North Africa')
GROUP BY (c.name, c.region, f.gni, m.year)
ORDER BY c.name, m.year


-- Dice + Roll Up (Country --> Region)
-- Yearly HDI & Population Growth where population growth was negative in the 2010 decade
SELECT
    case when RESULT.region is null then 'World' else RESULT.region end, 
    case when RESULT.name is null then 'Total' else RESULT.name end,  
    ROUND(CAST(AVG(RESULT.hdi) as numeric), 4) AS HDI, 
    ROUND(CAST(AVG(RESULT.population_growth) as numeric), 4) AS Pop_Growth 
FROM (
     SELECT DISTINCT C.name, C.region, F.hdi, M.year, P.p_growth_urban + P.p_growth_rural AS population_growth 
     FROM country C, wb_hnp F, month M, population P 
     WHERE C.country_key = F.country_key AND F.month_key = M.month_key AND P.population_key = F.population_key AND M.decade=2010 
     ORDER BY C.name, M.year
) AS RESULT 
WHERE 
  RESULT.population_growth <= 0 
GROUP BY ROLLUP(RESULT.region, RESULT.name);

-- Slice + Drill Down (Decade --> Year)
-- Yearly life satisfaction of North American Countries in the 2000 decade
SELECT DISTINCT 
   C.name, F.life_satis, M.year  
FROM 
    wb_hnp F, country C, month M
WHERE 
    F.country_key = C.country_key AND 
    region = 'North America' AND
    M.month_key = F.month_key AND 
	M.decade = 2000
GROUP BY
	C.name, F.life_satis, M.year  
ORDER BY C.name, M.year

-- Windowing
-- Compare each country’s last GNI with its income group’s average GNI over 16 years 
SELECT
    RESULT.name, RESULT.income_group, RESULT.gni AS gni_2020, AVG(RESULT.gni)
OVER (PARTITION BY RESULT.income_group) AS average_income_group_gni
FROM (
    SELECT DISTINCT C.name, C.income_group, F.gni, M.year
    FROM country C, wb_hnp F, month M
    WHERE M.month_key = F.month_key AND M.year = 2020 AND c.country_key = F.country_key
) as RESULT;

-- Window Clause
-- Moving average of each country HDI over 3 years
SELECT
    RESULT.name, 
    RESULT.year,
    hdi, 
    CAST(
        AVG(hdi) OVER W AS DECIMAL(3, 3)
    ) AS moving_3_year_hdi_avg 
FROM (
    SELECT DISTINCT C.name, M.year, F.hdi 
    FROM country C, wb_hnp F, month M 
    WHERE M.month_key = F.month_key AND C.country_key = F.country_key
) AS RESULT
WINDOW W AS (
    PARTITION BY RESULT.name 
    ORDER BY RESULT.year RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING
);