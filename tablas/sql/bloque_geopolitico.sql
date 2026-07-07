CREATE TABLE IA_PAISES.RAW.DIM_COUNTRIES (
    COUNTRY_ID   VARCHAR(10),
    COUNTRY_NAME VARCHAR(100),
    BLOC         VARCHAR(20),
    IS_NATO      BOOLEAN
);

INSERT INTO IA_PAISES.RAW.DIM_COUNTRIES VALUES
('ARG', 'Argentina',           'OTHER', FALSE),
('AUT', 'Austria',             'EU',    TRUE),
('BEL', 'Belgium',             'EU',    TRUE),
('CAN', 'Canada',              'NATO',  TRUE),
('CHL', 'Chile',               'OTHER', FALSE),
('CHN', 'China',               'BRICS', FALSE),
('DNK', 'Denmark',             'EU',    TRUE),
('FIN', 'Finland',             'EU',    TRUE),
('FRA', 'France',              'EU',    TRUE),
('DEU', 'Germany',             'EU',    TRUE),
('GRC', 'Greece',              'EU',    TRUE),
('HUN', 'Hungary',             'EU',    FALSE),
('IRL', 'Ireland',             'EU',    FALSE),
('ITA', 'Italy',               'EU',    TRUE),
('JPN', 'Japan',               'OTHER', FALSE),
('KOR', 'Korea, Rep.',         'OTHER', FALSE),
('LTU', 'Lithuania',           'EU',    TRUE),
('LUX', 'Luxembourg',          'EU',    TRUE),
('MEX', 'Mexico',              'OTHER', FALSE),
('NLD', 'Netherlands',         'EU',    TRUE),
('NOR', 'Norway',              'OTHER', TRUE),
('POL', 'Poland',              'EU',    TRUE),
('PRT', 'Portugal',            'EU',    TRUE),
('RUS', 'Russian Federation',  'BRICS', FALSE),
('SAU', 'Saudi Arabia',        'OTHER', FALSE),
('SVN', 'Slovenia',            'EU',    TRUE),
('ZAF', 'South Africa',        'BRICS', FALSE),
('ESP', 'Spain',               'EU',    TRUE),
('SWE', 'Sweden',              'EU',    TRUE),
('UKR', 'Ukraine',             'OTHER', FALSE),
('USA', 'United States',       'NATO',  TRUE);

CREATE OR REPLACE VIEW IA_PAISES.ANALYTICS.VW_AI_KPI AS
SELECT
    k.COUNTRY_ID,
    k.COUNTRY_NAME,
    k.YEAR,
    d.BLOC,
    d.IS_NATO,
    MAX(CASE WHEN k.INDICATOR = 'AI_PUBLICATIONS' THEN k.VALUE END)         AS AI_PUBLICATIONS,
    MAX(CASE WHEN k.INDICATOR = 'AI_PATENTS' THEN k.VALUE END)              AS AI_PATENTS,
    MAX(CASE WHEN k.INDICATOR = 'POPULATION' THEN k.VALUE END)              AS POPULATION,
    MAX(CASE WHEN k.INDICATOR = 'GDP_USD' THEN k.VALUE END)                 AS GDP_USD,
    MAX(CASE WHEN k.INDICATOR = 'GDP_PER_CAPITA_USD' THEN k.VALUE END)      AS GDP_PER_CAPITA_USD,
    MAX(CASE WHEN k.INDICATOR = 'RD_GDP_PCT' THEN k.VALUE END)              AS RD_GDP_PCT,
    MAX(CASE WHEN k.INDICATOR = 'RESEARCHERS_PER_MILLION' THEN k.VALUE END) AS RESEARCHERS_PER_MILLION
FROM (
    SELECT * FROM IA_PAISES.RAW.OPENALEX_PUBLICATIONS
    UNION ALL
    SELECT * FROM IA_PAISES.RAW.WORLDBANK_INDICATORS
    UNION ALL
    SELECT * FROM IA_PAISES.RAW.BIGQUERY_PATENTS
) k
LEFT JOIN IA_PAISES.RAW.DIM_COUNTRIES d ON k.COUNTRY_ID = d.COUNTRY_ID
GROUP BY k.COUNTRY_ID, k.COUNTRY_NAME, k.YEAR, d.BLOC, d.IS_NATO;


-- vista previa para ciertos paises
SELECT COUNTRY_NAME, BLOC, IS_NATO, YEAR, AI_PUBLICATIONS, RD_GDP_PCT
FROM IA_PAISES.ANALYTICS.VW_AI_KPI
WHERE COUNTRY_ID IN ('CHN','USA','DEU','KOR')
ORDER BY COUNTRY_NAME, YEAR;