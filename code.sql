-----------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------          DROP TABLES                 -----------------------------------------------------------------------
DROP VIEW vista_indicadores;
DROP VIEW vista_consolidado;
DROP TABLE aseguradoras;
DROP TABLE sexos;
DROP TABLE poblacion;
DROP TABLE localidades;
DROP TABLE programa_1;
DROP TABLE temp_programa_1;
DROP TABLE programa_2;
DROP TABLE temp_programa_2;
DROP TABLE programa_3;
DROP TABLE temp_programa_3;
DROP TABLE programa_4;
DROP TABLE temp_programa_4;






-----------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------         AUX TABLEES                 -----------------------------------------------------------------------


CREATE TABLE aseguradoras(
    palabra_clave VARCHAR(100),
    nombre VARCHAR(100)
);

INSERT INTO aseguradoras(palabra_clave, nombre)VALUES
(NULL, 'SIN DATO'),
('CAPITAL SALUD', 'CAPITAL SALUD E.P.S.'),
('NUEVA EPS', 'NUEVA EPS S.A'),
('OTR', 'OTROS'),
('SALUD TOTAL', 'SALUD TOTAL S.A'),
('PREPAGADA SURAMERICANA', 'EPS Y MEDICINA PREPAGADA SURAMERICANA S.A' ),
('FERROCARRILES NACIONAL', 'FERROCARRILES NACIONAL E.P.S.'),
('BOLIVAR', 'SALUD BOLIVAR E.P.S'),
('COMPENSAR', 'COMPENSAR E.P.S.'),
('SANITAS', 'E.P.S. SANITAS'),
('FAMISANAR', 'FAMISANAR E.P.S. LTDA - CAFAM - COLSUBSIDIO'),
('COLSUBSIDIO', 'FAMISANAR E.P.S. LTDA - CAFAM - COLSUBSIDIO'),
('CAFAM', 'FAMISANAR E.P.S. LTDA - CAFAM - COLSUBSIDIO'),
('ALIANSALUD', 'ALIANSALUD E.P.S.'),
('COOSALUD', 'COOSALUD E.P.S.'),
('SOS E.P.S', 'SOS E.P.S'),
('SOS EPS', 'SOS E.P.S'),
('MALLAMAS', 'MALLAMAS E.P.S.'),
('NO AFILIADO', 'SIN ASEGURAMIENTO');

CREATE TABLE sexos(
    palabra_clave VARCHAR(100),
    nombre VARCHAR(100)
);

INSERT INTO sexos(palabra_clave, nombre) VALUES
(NULL, 'NO REGISTRA'),
('1', 'HOMBRE'),
('2', 'MUJER'),
('3', 'INTERSEXUAL'),
('HOMBRE', 'HOMBRE'),
('MUJER', 'MUJER'),
('HOMBRES', 'HOMBRE'),
('MUJERES', 'MUJER'),
('MASCULINO', 'HOMBRE'),
('FEMENINO', 'MUJER'),
('INTERSEXUAL', 'INTERSEXUAL');


-----------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------
----HERE I CREATE A TABLE LOCALIDADES WITH THE INFO OF POBLACION SO WE CAN CONNECT THE LOCALIDADES FROM THE OTHER PROGRAMS WITH THE ONES

CREATE TABLE poblacion (
    ano INT,
    codigo_localidad INT,
    nombre_localidad VARCHAR(50),
    sexo VARCHAR(10),
    edad INT,
    curso_de_vida VARCHAR(20),
    grupo_edad VARCHAR(10),
    poblacion_7 INT
);

COPY poblacion (ano, codigo_localidad, nombre_localidad, sexo, edad, curso_de_vida, grupo_edad, poblacion_7)
FROM '/mnt/data/POBLACION.txt'
DELIMITER '|'
CSV HEADER;


CREATE TABLE localidades(
    codigo INT,
    palabra_clave VARCHAR(100),
    nombre VARCHAR(100)
);

INSERT INTO localidades(codigo, palabra_clave, nombre)
SELECT DISTINCT codigo_localidad, nombre_localidad, (codigo_localidad || ' - ' || nombre_localidad) 
FROM poblacion WHERE codigo_localidad != 0;

INSERT INTO localidades(codigo, palabra_clave, nombre) VALUES
(0, 'Bogota', '99 - Localidad Desconocida'),
(99, 'Bogota', '99 - Localidad Desconocida');




-----------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------

-------------------                                            PROCESAMIENTO PROGRAMA 1                    ------------------------------------

CREATE TABLE temp_programa_1(
    regimen_afiliacion VARCHAR(100),
    localidad_calculada VARCHAR(50),
    asegurador VARCHAR(100),
    fecha_nacimiento TEXT,
    sexo VARCHAR(50),
    fecha_consulta TEXT,
    nacionalidad VARCHAR(50) 
);

COPY temp_programa_1 (regimen_afiliacion, localidad_calculada, asegurador, fecha_nacimiento, sexo, fecha_consulta, nacionalidad)
FROM '/mnt/data/PROGRAMA_1.txt'
DELIMITER ','
CSV HEADER;

CREATE TABLE programa_1(
    localidad VARCHAR(50),
    eapb VARCHAR(100),
    edad INT,
    sexo VARCHAR(50),
    fecha_caracterizacion DATE
);


----actual insertion

INSERT INTO programa_1 (
    localidad, 
    eapb,
    edad, 
    sexo, 
    fecha_caracterizacion
)
SELECT 
    COALESCE(
        (SELECT loc.nombre
        FROM localidades loc
        WHERE tm.localidad_calculada ILIKE ('%' || loc.palabra_clave || '%')
        LIMIT 1
        ), '99 - Localidad desconocida') AS localidad,
    COALESCE(
        (SELECT aseg.nombre
        FROM aseguradoras aseg
        WHERE tm.asegurador ILIKE ('%' || aseg.palabra_clave || '%')
        LIMIT 1
        ), 'OTROS') AS eapb,
    CASE 
        WHEN tm.fecha_nacimiento ~ '^(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[0-2])/[0-9]{4}$'
             AND (
                 (substring(tm.fecha_nacimiento FROM '^[0-3]?[0-9]')::int <= 31) AND  
                 (substring(tm.fecha_nacimiento FROM '/(0?[1-9]|1[0-2])/')::int <= 12) AND  
                 (
                     (substring(tm.fecha_nacimiento FROM '/(0?2)/') IS NULL OR substring(tm.fecha_nacimiento FROM '^[0-3]?[0-9]')::int <= 29) OR 
                     (substring(tm.fecha_nacimiento FROM '/(0?[469]|11)/') IS NULL OR substring(tm.fecha_nacimiento FROM '^[0-3]?[0-9]')::int <= 30) 
                 )
             )
            THEN EXTRACT(YEAR FROM AGE(TO_DATE(tm.fecha_nacimiento, 'DD/MM/YYYY')))
        ELSE NULL 
    END AS edad,
    CASE WHEN tm.sexo = 'MASCULINO' THEN 'HOMBRE' ELSE 'MUJER' END AS sexo,
    CASE 
        WHEN tm.fecha_consulta ~ '^(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[0-2])/[0-9]{4}$'
             AND (
                 (substring(tm.fecha_consulta FROM '^[0-3]?[0-9]')::int <= 31) AND  
                 (substring(tm.fecha_consulta FROM '/(0?[1-9]|1[0-2])/')::int <= 12) AND 
                 (
                     (substring(tm.fecha_consulta FROM '/(0?2)/') IS NULL OR substring(tm.fecha_consulta FROM '^[0-3]?[0-9]')::int <= 29) OR 
                     (substring(tm.fecha_consulta FROM '/(0?[469]|11)/') IS NULL OR substring(tm.fecha_consulta FROM '^[0-3]?[0-9]')::int <= 30)  
                 )
             )
            THEN TO_DATE(tm.fecha_consulta, 'DD/MM/YYYY')
        ELSE NULL 
    END AS fecha_caracterizacion
FROM temp_programa_1 tm;


DROP TABLE temp_programa_1;



-----------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------         PROCESAMIENTO PROGRAMA 2        ---------------------------------------------------------------

CREATE TABLE temp_programa_2(
    sexo_biologico VARCHAR(100),
    localidad VARCHAR(100),
    eapb VARCHAR(100),
    fecha_nacimiento TEXT ,
    pertenencia_etnica VARCHAR(100),
    sexo_biologico_1 VARCHAR(100) ,
    riesgo_psicosocial VARCHAR(10),
    fecha_consulta TEXT,
    talla TEXT
);

COPY temp_programa_2 (sexo_biologico, localidad, eapb, fecha_nacimiento, pertenencia_etnica, sexo_biologico_1, riesgo_psicosocial,fecha_consulta, talla )
FROM '/mnt/data/PROGRAMA_2.txt'
DELIMITER '|'
CSV HEADER;

CREATE TABLE programa_2(
    sexo VARCHAR(100),
    localidad VARCHAR(100),
    eapb VARCHAR(100),
    edad INT,
    fecha_caracterizacion DATE
);


------ actual insertion

INSERT INTO programa_2 (
    sexo, 
    localidad, 
    eapb, 
    edad, 
    fecha_caracterizacion
)
SELECT 
    COALESCE(
        (SELECT sex.nombre
        FROM sexos sex
        WHERE tm.sexo_biologico ILIKE ('%' || sex.palabra_clave || '%')
        LIMIT 1
        ), 'NO REGISTRA') AS sexo,
    COALESCE(
        (SELECT loc.nombre
        FROM localidades loc
        WHERE tm.localidad ILIKE ('%' || loc.palabra_clave || '%')
        LIMIT 1
        ), '99 - Localidad desconocida') AS localidad,
    'SIN DATO',
    CASE 
        WHEN tm.fecha_nacimiento ~ '^(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[0-2])/[0-9]{4}$'
             AND (
                 (substring(tm.fecha_nacimiento FROM '^[0-3]?[0-9]')::int <= 31) AND  
                 (substring(tm.fecha_nacimiento FROM '/(0?[1-9]|1[0-2])/')::int <= 12) AND  
                 (
                     (substring(tm.fecha_nacimiento FROM '/(0?2)/') IS NULL OR substring(tm.fecha_nacimiento FROM '^[0-3]?[0-9]')::int <= 29) OR 
                     (substring(tm.fecha_nacimiento FROM '/(0?[469]|11)/') IS NULL OR substring(tm.fecha_nacimiento FROM '^[0-3]?[0-9]')::int <= 30) 
                 )
             )
            THEN EXTRACT(YEAR FROM AGE(TO_DATE(tm.fecha_nacimiento, 'DD/MM/YYYY')))
        ELSE NULL 
    END AS edad,
    CASE 
        WHEN tm.fecha_consulta ~ '^(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[0-2])/[0-9]{4}$'
             AND (
                 (substring(tm.fecha_consulta FROM '^[0-3]?[0-9]')::int <= 31) AND  
                 (substring(tm.fecha_consulta FROM '/(0?[1-9]|1[0-2])/')::int <= 12) AND 
                 (
                     (substring(tm.fecha_consulta FROM '/(0?2)/') IS NULL OR substring(tm.fecha_consulta FROM '^[0-3]?[0-9]')::int <= 29) OR 
                     (substring(tm.fecha_consulta FROM '/(0?[469]|11)/') IS NULL OR substring(tm.fecha_consulta FROM '^[0-3]?[0-9]')::int <= 30)  
                 )
             )
            THEN TO_DATE(tm.fecha_consulta, 'DD/MM/YYYY')
        ELSE NULL 
    END AS fecha_caracterizacion
FROM temp_programa_2 tm;


DROP TABLE temp_programa_2;


-----------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------         PROCESAMIENTO PROGRAMA 3        ---------------------------------------------------------------


CREATE TABLE temp_programa_3(
    localidadfic_3 VARCHAR(100),
    nacionalidad_10 VARCHAR(100),
    nombreeapb_27 VARCHAR(100),
    fecha_nacimiento_14 TEXT,
    etnia_18 VARCHAR(100),
    sexo_11 VARCHAR(100),
    genero_12 VARCHAR(100),
    fecha_intervencion_2 TEXT
);

COPY temp_programa_3 (localidadfic_3, nacionalidad_10, nombreeapb_27, fecha_nacimiento_14, etnia_18, sexo_11, genero_12,fecha_intervencion_2)
FROM '/mnt/data/PROGRAMA_3.txt'
DELIMITER '|'
CSV HEADER;


CREATE TABLE programa_3(
    localidad VARCHAR(100),
    eapb VARCHAR(100),
    edad INT,
    sexo VARCHAR(100),
    fecha_caracterizacion DATE
);


-----actual insertion

INSERT INTO programa_3 (
    localidad, 
    eapb, 
    edad, 
    sexo, 
    fecha_caracterizacion
)
SELECT 
    COALESCE(
        (SELECT loc.nombre
        FROM localidades loc
        WHERE tm.localidadfic_3 ILIKE ('%' || loc.palabra_clave || '%')
        LIMIT 1
        ), '99 - Localidad desconocida') AS localidad,
    COALESCE(
        (SELECT aseg.nombre
        FROM aseguradoras aseg
        WHERE tm.nombreeapb_27 ILIKE ('%' || aseg.palabra_clave || '%')
        LIMIT 1
        ), 'OTROS') AS eapb,
    CASE 
        WHEN fecha_nacimiento_14 ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$' 
            THEN EXTRACT( YEAR FROM AGE(TO_TIMESTAMP(fecha_nacimiento_14, 'YYYY-MM-DD HH24:MI:SS')::DATE)) 
        ELSE NULL 
    END AS edad,
    COALESCE(
        (SELECT sex.nombre
        FROM sexos sex
        WHERE tm.sexo_11 ILIKE ('%' || sex.palabra_clave || '%')
        LIMIT 1
        ), 'NO REGISTRA') AS sexo,
    TO_TIMESTAMP(fecha_intervencion_2, 'YYYYMMDD HH24MISS')::DATE AS fecha_caracterizacion
FROM temp_programa_3 tm;


DROP TABLE temp_programa_3;


-----------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------         PROCESAMIENTO PROGRAMA 4        ---------------------------------------------------------------

CREATE TABLE temp_programa_4(
    localidad_fic INTEGER,
    estado_civil VARCHAR(100),
    nombre_eapb VARCHAR(100),
    fecha_nacimiento TEXT,
    etnia VARCHAR(100),
    profesion VARCHAR(100),
    fecha_intervencion TEXT
);


COPY temp_programa_4 (localidad_fic, estado_civil, nombre_eapb, fecha_nacimiento, etnia, profesion,fecha_intervencion)
FROM '/mnt/data/PROGRAMA_4.txt'
DELIMITER '|'
CSV HEADER;


CREATE TABLE programa_4(
    localidad VARCHAR(100),
    eapb VARCHAR(100),
    edad INT,
    sexo VARCHAR(100),
    fecha_caracterizacion DATE
);

INSERT INTO programa_4(
    localidad, 
    eapb, 
    edad, 
    sexo, 
    fecha_caracterizacion
)
SELECT 
    COALESCE(
        (SELECT loc.nombre
        FROM localidades loc
        WHERE tm.localidad_fic = loc.codigo
        LIMIT 1
        ), '99 - Localidad desconocida') AS localidad,
    COALESCE(
        (SELECT aseg.nombre
        FROM aseguradoras aseg
        WHERE tm.nombre_eapb ILIKE ('%' || aseg.palabra_clave || '%')
        LIMIT 1
        ), 'OTROS') AS eapb,
    CASE 
        WHEN tm.fecha_nacimiento ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$' 
            THEN EXTRACT(YEAR FROM AGE(TO_TIMESTAMP(tm.fecha_nacimiento, 'YYYY-MM-DD HH24:MI:SS')::DATE ))
        ELSE NULL 
    END AS edad,
    'NO REGISTRA',
    CASE 
        WHEN tm.fecha_intervencion ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$' 
            THEN TO_TIMESTAMP(tm.fecha_intervencion, 'YYYY-MM-DD HH24:MI:SS')::DATE 
        ELSE NULL 
    END AS fecha_caracterizacion
FROM temp_programa_4 tm;


DROP TABLE temp_programa_4;



-----------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------          VISTA FINAL                     -----------------------------------------------------------

CREATE VIEW vista_consolidado AS
SELECT localidad, eapb, edad, sexo, 'PROGRAMA 4' AS programa, fecha_caracterizacion
FROM programa_4
UNION ALL
SELECT localidad, eapb, edad, sexo,'PROGRAMA 3' AS programa, fecha_caracterizacion
FROM programa_3
UNION ALL
SELECT localidad, eapb, edad, sexo,'PROGRAMA 2' AS programa, fecha_caracterizacion
FROM programa_2
UNION ALL
SELECT localidad, eapb, edad, sexo,'PROGRAMA 1' AS programa, fecha_caracterizacion
FROM programa_1;


--generar archivo txt con el resultado
COPY (SELECT * FROM vista_consolidado) TO '/mnt/data/resultado.txt' (FORMAT text, DELIMITER '|', HEADER);






-----------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------          VISTA INDICADORES               -----------------------------------------------------------


CREATE VIEW vista_indicadores AS
WITH atendidos AS(
    SELECT DATE_PART('year', subat.fecha) AS ano ,
    COUNT(*) AS total,
    subat.grupo_edad as grupo_edad, subat.cod_loc as cod_loc,
    subat.localidad as localidad
    FROM (
        SELECT vs.localidad AS localidad,
            vs.fecha_caracterizacion as fecha,
            CASE WHEN vs.localidad != '99 - Localidad desconocida' THEN CAST(LEFT(vs.localidad, 2) AS INT) ELSE 0 END AS cod_loc,
            (
                SELECT p.grupo_edad
                FROM poblacion p
                WHERE 
                    CASE 
                        WHEN p.grupo_edad = '100 o más' THEN vs.edad >= 100
                        ELSE vs.edad >= CAST(LEFT(p.grupo_edad, 2) AS INT)
                                AND vs.edad <= CAST(SUBSTRING(p.grupo_edad, 6, 2) AS INT)
                    END
                LIMIT 1
            ) AS grupo_edad
        FROM vista_consolidado vs
    ) subat
    GROUP BY DATE_PART('year', subat.fecha), subat.grupo_edad, subat.cod_loc, subat.localidad
),
pobla AS (
    SELECT 
        subpob.cod_loc as cod_loc,
        subpob.ano as ano, 
        subpob.grupo_edad as grupo_edad,
        SUM(subpob.poblacion_7) as poblacion
    FROM (SELECT
        ano,
        CASE WHEN codigo_localidad != 0 THEN codigo_localidad ELSE 99 END AS cod_loc,
        grupo_edad,
        poblacion_7
        FROM poblacion pob ) AS subpob
    GROUP BY subpob.cod_loc, subpob.ano, subpob.grupo_edad
)
SELECT  pob.ano AS año, 
        ate.localidad AS localidad, 
        pob.grupo_edad AS quinquenio, 
        SUM(pob.poblacion) AS poblacion, 
        SUM(ate.total) AS atendidos,
        SUM(ate.total)/SUM(pob.poblacion) AS indicador_atendidos
FROM pobla pob
INNER JOIN atendidos ate ON ate.grupo_edad = pob.grupo_edad AND  ate.cod_loc = pob.cod_loc AND ate.ano = pob.ano
GROUP BY pob.grupo_edad, ate.localidad, pob.ano;

--generar archivo txt con el resultado
COPY (SELECT * FROM vista_indicadores) TO '/mnt/data/resultado_indicadores.txt' (FORMAT text, DELIMITER '|', HEADER);

