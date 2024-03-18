--
-- NOTE:
--
-- File paths need to be edited. Search for $$PATH$$ and
-- replace it with the path to the directory containing
-- the extracted data files.
--
--
-- PostgreSQL database dump
--


SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;


SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

CREATE SCHEMA IF NOT EXISTS dvdrental;
DROP TABLE IF EXISTS dvdrental.actor CASCADE;
DROP TABLE IF EXISTS dvdrental.film CASCADE;
DROP TABLE IF EXISTS dvdrental.address CASCADE;
DROP TABLE IF EXISTS dvdrental.country CASCADE;
DROP TABLE IF EXISTS dvdrental.category CASCADE;
DROP TABLE IF EXISTS dvdrental.payment CASCADE;
DROP TABLE IF EXISTS dvdrental.rental CASCADE;
DROP TABLE IF EXISTS dvdrental.staff CASCADE;
DROP TABLE IF EXISTS dvdrental.store CASCADE;
DROP TABLE IF EXISTS dvdrental.customer CASCADE;
DROP TABLE IF EXISTS dvdrental.film_actor CASCADE;
DROP TABLE IF EXISTS dvdrental.city CASCADE;
DROP TABLE IF EXISTS dvdrental.film_category CASCADE;
DROP TABLE IF EXISTS dvdrental.inventory CASCADE;
DROP TABLE IF EXISTS dvdrental.language CASCADE;
DROP SEQUENCE IF EXISTS dvdrental.actor_actor_id_seq CASCADE;
DROP SEQUENCE IF EXISTS dvdrental.address_address_id_seq CASCADE;
DROP SEQUENCE IF EXISTS dvdrental.category_category_id_seq CASCADE;
DROP SEQUENCE IF EXISTS dvdrental.city_city_id_seq CASCADE;
DROP SEQUENCE IF EXISTS dvdrental.country_country_id_seq CASCADE;
DROP SEQUENCE IF EXISTS dvdrental.customer_customer_id_seq CASCADE;
DROP SEQUENCE IF EXISTS dvdrental.film_film_id_seq CASCADE;
DROP SEQUENCE IF EXISTS dvdrental.inventory_inventory_id_seq CASCADE;
DROP SEQUENCE IF EXISTS dvdrental.language_language_id_seq CASCADE;
DROP SEQUENCE IF EXISTS dvdrental.payment_payment_id_seq CASCADE;
DROP SEQUENCE IF EXISTS dvdrental.rental_rental_id_seq CASCADE;
DROP SEQUENCE IF EXISTS dvdrental.staff_staff_id_seq CASCADE;
DROP SEQUENCE IF EXISTS dvdrental.store_store_id_seq CASCADE;
DROP TYPE IF EXISTS dvdrental.mpaa_rating CASCADE;
DROP DOMAIN IF EXISTS dvdrental.year CASCADE;
DROP FUNCTION IF EXISTS dvdrental._group_concat(text, text) CASCADE;
DROP FUNCTION IF EXISTS dvdrental.last_updated() CASCADE;



--
-- Name: mpaa_rating; Type: TYPE; Schema: dvdrental; 
--

CREATE TYPE dvdrental.mpaa_rating AS ENUM (
    'G',
    'PG',
    'PG-13',
    'R',
    'NC-17'
);



--
-- Name: year; Type: DOMAIN; Schema: dvdrental; 
--

CREATE DOMAIN dvdrental.year AS integer
	CONSTRAINT year_check CHECK (((VALUE >= 1901) AND (VALUE <= 2155)));



--
-- Name: _group_concat(text, text); Type: FUNCTION; Schema: dvdrental; 
--

CREATE FUNCTION dvdrental._group_concat(text, text) RETURNS text
    LANGUAGE sql IMMUTABLE
    AS $_$
SELECT CASE
  WHEN $2 IS NULL THEN $1
  WHEN $1 IS NULL THEN $2
  ELSE $1 || ', ' || $2
END
$_$;

-- 
-- Name: last_updated(); Type: FUNCTION; Schema: dvdrental; 
--

CREATE FUNCTION dvdrental.last_updated() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.last_update = CURRENT_TIMESTAMP;
    RETURN NEW;
END $$;



--
-- Name: customer_customer_id_seq; Type: SEQUENCE; Schema: dvdrental; 
--

CREATE SEQUENCE dvdrental.customer_customer_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: customer; Type: TABLE; Schema: dvdrental; 
--

CREATE TABLE dvdrental.customer (
    customer_id integer DEFAULT nextval('dvdrental.customer_customer_id_seq'::regclass) NOT NULL,
    store_id smallint NOT NULL,
    first_name character varying(45) NOT NULL,
    last_name character varying(45) NOT NULL,
    email character varying(50),
    address_id smallint NOT NULL,
    activebool boolean DEFAULT true NOT NULL,
    create_date date DEFAULT ('now'::text)::date NOT NULL,
    last_update timestamp without time zone DEFAULT now(),
    active integer
);



--
-- Name: rewards_report(integer, numeric); Type: FUNCTION; Schema: dvdrental; 
--

CREATE FUNCTION dvdrental.rewards_report(min_monthly_purchases integer, min_dollar_amount_purchased numeric) RETURNS SETOF dvdrental.customer
    LANGUAGE plpgsql SECURITY DEFINER
    AS $_$
DECLARE
    last_month_start DATE;
    last_month_end DATE;
rr RECORD;
tmpSQL TEXT;
BEGIN

    /* Some sanity checks... */
    IF min_monthly_purchases = 0 THEN
        RAISE EXCEPTION 'Minimum monthly purchases parameter must be > 0';
    END IF;
    IF min_dollar_amount_purchased = 0.00 THEN
        RAISE EXCEPTION 'Minimum monthly dollar amount purchased parameter must be > $0.00';
    END IF;

    last_month_start := CURRENT_DATE - '3 month'::interval;
    last_month_start := to_date((extract(YEAR FROM last_month_start) || '-' || extract(MONTH FROM last_month_start) || '-01'),'YYYY-MM-DD');
    last_month_end := LAST_DAY(last_month_start);

    /*
    Create a temporary storage area for Customer IDs.
    */
    CREATE TEMPORARY TABLE tmpCustomer (customer_id INTEGER NOT NULL PRIMARY KEY);

    /*
    Find all customers meeting the monthly purchase requirements
    */

    tmpSQL := 'INSERT INTO tmpCustomer (customer_id)
        SELECT p.customer_id
        FROM payment AS p
        WHERE DATE(p.payment_date) BETWEEN '||quote_literal(last_month_start) ||' AND '|| quote_literal(last_month_end) || '
        GROUP BY customer_id
        HAVING SUM(p.amount) > '|| min_dollar_amount_purchased || '
        AND COUNT(customer_id) > ' ||min_monthly_purchases ;

    EXECUTE tmpSQL;

    /*
    Output ALL customer information of matching rewardees.
    Customize output as needed.
    */
    FOR rr IN EXECUTE 'SELECT c.* FROM tmpCustomer AS t INNER JOIN customer AS c ON t.customer_id = c.customer_id' LOOP
        RETURN NEXT rr;
    END LOOP;

    /* Clean up */
    tmpSQL := 'DROP TABLE tmpCustomer';
    EXECUTE tmpSQL;

RETURN;
END
$_$;



--
-- Name: group_concat(text); Type: AGGREGATE; Schema: dvdrental; 
--

CREATE AGGREGATE dvdrental.group_concat(text) (
    SFUNC = dvdrental._group_concat,
    STYPE = text
);



--
-- Name: actor_actor_id_seq; Type: SEQUENCE; Schema: dvdrental; 
--

CREATE SEQUENCE dvdrental.actor_actor_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



--
-- Name: actor; Type: TABLE; Schema: dvdrental; 
--

CREATE TABLE dvdrental.actor (
    actor_id integer DEFAULT nextval('dvdrental.actor_actor_id_seq'::regclass) NOT NULL,
    first_name character varying(45) NOT NULL,
    last_name character varying(45) NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);



--
-- Name: category_category_id_seq; Type: SEQUENCE; Schema: dvdrental; 
--

CREATE SEQUENCE dvdrental.category_category_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



--
-- Name: category; Type: TABLE; Schema: dvdrental; 
--

CREATE TABLE dvdrental.category (
    category_id integer DEFAULT nextval('dvdrental.category_category_id_seq'::regclass) NOT NULL,
    name character varying(25) NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);



--
-- Name: film_film_id_seq; Type: SEQUENCE; Schema: dvdrental; 
--

CREATE SEQUENCE dvdrental.film_film_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



--
-- Name: film; Type: TABLE; Schema: dvdrental; 
--

CREATE TABLE dvdrental.film (
    film_id integer DEFAULT nextval('dvdrental.film_film_id_seq'::regclass) NOT NULL,
    title character varying(255) NOT NULL,
    description text,
    release_year dvdrental.year,
    language_id smallint NOT NULL,
    rental_duration smallint DEFAULT 3 NOT NULL,
    rental_rate numeric(4,2) DEFAULT 4.99 NOT NULL,
    length smallint,
    replacement_cost numeric(5,2) DEFAULT 19.99 NOT NULL,
    rating dvdrental.mpaa_rating DEFAULT 'G'::dvdrental.mpaa_rating,
    last_update timestamp without time zone DEFAULT now() NOT NULL,
    special_features text[],
    fulltext tsvector NOT NULL
);



--
-- Name: film_actor; Type: TABLE; Schema: dvdrental; 
--

CREATE TABLE dvdrental.film_actor (
    actor_id smallint NOT NULL,
    film_id smallint NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);



--
-- Name: film_category; Type: TABLE; Schema: dvdrental; 
--

CREATE TABLE dvdrental.film_category (
    film_id smallint NOT NULL,
    category_id smallint NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);



--
-- Name: actor_info; Type: VIEW; Schema: dvdrental; 
--

CREATE VIEW dvdrental.actor_info AS
 SELECT a.actor_id,
    a.first_name,
    a.last_name,
    dvdrental.group_concat(DISTINCT (((c.name)::text || ': '::text) || ( SELECT dvdrental.group_concat((f.title)::text) AS group_concat
           FROM ((dvdrental.film f
             JOIN dvdrental.film_category fc_1 ON ((f.film_id = fc_1.film_id)))
             JOIN dvdrental.film_actor fa_1 ON ((f.film_id = fa_1.film_id)))
          WHERE ((fc_1.category_id = c.category_id) AND (fa_1.actor_id = a.actor_id))
          GROUP BY fa_1.actor_id))) AS film_info
   FROM (((dvdrental.actor a
     LEFT JOIN dvdrental.film_actor fa ON ((a.actor_id = fa.actor_id)))
     LEFT JOIN dvdrental.film_category fc ON ((fa.film_id = fc.film_id)))
     LEFT JOIN dvdrental.category c ON ((fc.category_id = c.category_id)))
  GROUP BY a.actor_id, a.first_name, a.last_name;



--
-- Name: address_address_id_seq; Type: SEQUENCE; Schema: dvdrental; 
--

CREATE SEQUENCE dvdrental.address_address_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



--
-- Name: address; Type: TABLE; Schema: dvdrental; 
--

CREATE TABLE dvdrental.address (
    address_id integer DEFAULT nextval('dvdrental.address_address_id_seq'::regclass) NOT NULL,
    address character varying(50) NOT NULL,
    address2 character varying(50),
    district character varying(20) NOT NULL,
    city_id smallint NOT NULL,
    postal_code character varying(10),
    phone character varying(20) NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);



--
-- Name: city_city_id_seq; Type: SEQUENCE; Schema: dvdrental; 
--

CREATE SEQUENCE dvdrental.city_city_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



--
-- Name: city; Type: TABLE; Schema: dvdrental; 
--

CREATE TABLE dvdrental.city (
    city_id integer DEFAULT nextval('dvdrental.city_city_id_seq'::regclass) NOT NULL,
    city character varying(50) NOT NULL,
    country_id smallint NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);



--
-- Name: country_country_id_seq; Type: SEQUENCE; Schema: dvdrental; 
--

CREATE SEQUENCE dvdrental.country_country_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



--
-- Name: country; Type: TABLE; Schema: dvdrental; 
--

CREATE TABLE dvdrental.country (
    country_id integer DEFAULT nextval('dvdrental.country_country_id_seq'::regclass) NOT NULL,
    country character varying(50) NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);



--
-- Name: customer_list; Type: VIEW; Schema: dvdrental; 
--

CREATE VIEW dvdrental.customer_list AS
 SELECT cu.customer_id AS id,
    (((cu.first_name)::text || ' '::text) || (cu.last_name)::text) AS name,
    a.address,
    a.postal_code AS "zip code",
    a.phone,
    city.city,
    country.country,
        CASE
            WHEN cu.activebool THEN 'active'::text
            ELSE ''::text
        END AS notes,
    cu.store_id AS sid
   FROM (((dvdrental.customer cu
     JOIN dvdrental.address a ON ((cu.address_id = a.address_id)))
     JOIN dvdrental.city ON ((a.city_id = city.city_id)))
     JOIN dvdrental.country ON ((city.country_id = country.country_id)));



--
-- Name: film_list; Type: VIEW; Schema: dvdrental; 
--

CREATE VIEW dvdrental.film_list AS
 SELECT film.film_id AS fid,
    film.title,
    film.description,
    category.name AS category,
    film.rental_rate AS price,
    film.length,
    film.rating,
    dvdrental.group_concat((((actor.first_name)::text || ' '::text) || (actor.last_name)::text)) AS actors
   FROM ((((dvdrental.category
     LEFT JOIN dvdrental.film_category ON ((category.category_id = film_category.category_id)))
     LEFT JOIN dvdrental.film ON ((film_category.film_id = film.film_id)))
     JOIN dvdrental.film_actor ON ((film.film_id = film_actor.film_id)))
     JOIN dvdrental.actor ON ((film_actor.actor_id = actor.actor_id)))
  GROUP BY film.film_id, film.title, film.description, category.name, film.rental_rate, film.length, film.rating;



--
-- Name: inventory_inventory_id_seq; Type: SEQUENCE; Schema: dvdrental; 
--

CREATE SEQUENCE dvdrental.inventory_inventory_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



--
-- Name: inventory; Type: TABLE; Schema: dvdrental; 
--

CREATE TABLE dvdrental.inventory (
    inventory_id integer DEFAULT nextval('dvdrental.inventory_inventory_id_seq'::regclass) NOT NULL,
    film_id smallint NOT NULL,
    store_id smallint NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);



--
-- Name: language_language_id_seq; Type: SEQUENCE; Schema: dvdrental; 
--

CREATE SEQUENCE dvdrental.language_language_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



--
-- Name: language; Type: TABLE; Schema: dvdrental; 
--

CREATE TABLE dvdrental.language (
    language_id integer DEFAULT nextval('dvdrental.language_language_id_seq'::regclass) NOT NULL,
    name character(20) NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);



--
-- Name: nicer_but_slower_film_list; Type: VIEW; Schema: dvdrental; 
--

CREATE VIEW dvdrental.nicer_but_slower_film_list AS
 SELECT film.film_id AS fid,
    film.title,
    film.description,
    category.name AS category,
    film.rental_rate AS price,
    film.length,
    film.rating,
    dvdrental.group_concat((((upper("substring"((actor.first_name)::text, 1, 1)) || lower("substring"((actor.first_name)::text, 2))) || upper("substring"((actor.last_name)::text, 1, 1))) || lower("substring"((actor.last_name)::text, 2)))) AS actors
   FROM ((((dvdrental.category
     LEFT JOIN dvdrental.film_category ON ((category.category_id = film_category.category_id)))
     LEFT JOIN dvdrental.film ON ((film_category.film_id = film.film_id)))
     JOIN dvdrental.film_actor ON ((film.film_id = film_actor.film_id)))
     JOIN dvdrental.actor ON ((film_actor.actor_id = actor.actor_id)))
  GROUP BY film.film_id, film.title, film.description, category.name, film.rental_rate, film.length, film.rating;



--
-- Name: payment_payment_id_seq; Type: SEQUENCE; Schema: dvdrental; 
--

CREATE SEQUENCE dvdrental.payment_payment_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



--
-- Name: payment; Type: TABLE; Schema: dvdrental; 
--

CREATE TABLE dvdrental.payment (
    payment_id integer DEFAULT nextval('dvdrental.payment_payment_id_seq'::regclass) NOT NULL,
    customer_id smallint NOT NULL,
    staff_id smallint NOT NULL,
    rental_id integer NOT NULL,
    amount numeric(5,2) NOT NULL,
    payment_date timestamp without time zone NOT NULL
);



--
-- Name: rental_rental_id_seq; Type: SEQUENCE; Schema: dvdrental; 
--

CREATE SEQUENCE dvdrental.rental_rental_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



--
-- Name: rental; Type: TABLE; Schema: dvdrental; 
--

CREATE TABLE dvdrental.rental (
    rental_id integer DEFAULT nextval('dvdrental.rental_rental_id_seq'::regclass) NOT NULL,
    rental_date timestamp without time zone NOT NULL,
    inventory_id integer NOT NULL,
    customer_id smallint NOT NULL,
    return_date timestamp without time zone,
    staff_id smallint NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);



--
-- Name: sales_by_film_category; Type: VIEW; Schema: dvdrental; 
--

CREATE VIEW dvdrental.sales_by_film_category AS
 SELECT c.name AS category,
    sum(p.amount) AS total_sales
   FROM (((((dvdrental.payment p
     JOIN dvdrental.rental r ON ((p.rental_id = r.rental_id)))
     JOIN dvdrental.inventory i ON ((r.inventory_id = i.inventory_id)))
     JOIN dvdrental.film f ON ((i.film_id = f.film_id)))
     JOIN dvdrental.film_category fc ON ((f.film_id = fc.film_id)))
     JOIN dvdrental.category c ON ((fc.category_id = c.category_id)))
  GROUP BY c.name
  ORDER BY (sum(p.amount)) DESC;



--
-- Name: staff_staff_id_seq; Type: SEQUENCE; Schema: dvdrental; 
--

CREATE SEQUENCE dvdrental.staff_staff_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



--
-- Name: staff; Type: TABLE; Schema: dvdrental; 
--

CREATE TABLE dvdrental.staff (
    staff_id integer DEFAULT nextval('dvdrental.staff_staff_id_seq'::regclass) NOT NULL,
    first_name character varying(45) NOT NULL,
    last_name character varying(45) NOT NULL,
    address_id smallint NOT NULL,
    email character varying(50),
    store_id smallint NOT NULL,
    active boolean DEFAULT true NOT NULL,
    username character varying(16) NOT NULL,
    password character varying(40),
    last_update timestamp without time zone DEFAULT now() NOT NULL,
    picture bytea
);



--
-- Name: store_store_id_seq; Type: SEQUENCE; Schema: dvdrental; 
--

CREATE SEQUENCE dvdrental.store_store_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



--
-- Name: store; Type: TABLE; Schema: dvdrental; 
--

CREATE TABLE dvdrental.store (
    store_id integer DEFAULT nextval('dvdrental.store_store_id_seq'::regclass) NOT NULL,
    manager_staff_id smallint NOT NULL,
    address_id smallint NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);



--
-- Name: sales_by_store; Type: VIEW; Schema: dvdrental; 
--

CREATE VIEW dvdrental.sales_by_store AS
 SELECT (((c.city)::text || ','::text) || (cy.country)::text) AS store,
    (((m.first_name)::text || ' '::text) || (m.last_name)::text) AS manager,
    sum(p.amount) AS total_sales
   FROM (((((((dvdrental.payment p
     JOIN dvdrental.rental r ON ((p.rental_id = r.rental_id)))
     JOIN dvdrental.inventory i ON ((r.inventory_id = i.inventory_id)))
     JOIN dvdrental.store s ON ((i.store_id = s.store_id)))
     JOIN dvdrental.address a ON ((s.address_id = a.address_id)))
     JOIN dvdrental.city c ON ((a.city_id = c.city_id)))
     JOIN dvdrental.country cy ON ((c.country_id = cy.country_id)))
     JOIN dvdrental.staff m ON ((s.manager_staff_id = m.staff_id)))
  GROUP BY cy.country, c.city, s.store_id, m.first_name, m.last_name
  ORDER BY cy.country, c.city;



--
-- Name: staff_list; Type: VIEW; Schema: dvdrental; 
--

CREATE VIEW dvdrental.staff_list AS
 SELECT s.staff_id AS id,
    (((s.first_name)::text || ' '::text) || (s.last_name)::text) AS name,
    a.address,
    a.postal_code AS "zip code",
    a.phone,
    city.city,
    country.country,
    s.store_id AS sid
   FROM (((dvdrental.staff s
     JOIN dvdrental.address a ON ((s.address_id = a.address_id)))
     JOIN dvdrental.city ON ((a.city_id = city.city_id)))
     JOIN dvdrental.country ON ((city.country_id = country.country_id)));



--
-- Data for Name: actor; Type: TABLE DATA; Schema: dvdrental; 
--

\COPY dvdrental.actor (actor_id, first_name, last_name, last_update) FROM './3057.dat';

--
--

\COPY dvdrental.address (address_id, address, address2, district, city_id, postal_code, phone, last_update) FROM '3065.dat';

--
-- Data for Name: category; Type: TABLE DATA; Schema: dvdrental; 
--

\COPY dvdrental.category (category_id, name, last_update) FROM '3059.dat';

--
-- Data for Name: city; Type: TABLE DATA; Schema: dvdrental; 
--

\COPY dvdrental.city (city_id, city, country_id, last_update) FROM '3067.dat';

--
--

\COPY dvdrental.country (country_id, country, last_update) FROM '3069.dat';

--
-- Data for Name: customer; Type: TABLE DATA; Schema: dvdrental; 
--

\COPY dvdrental.customer (customer_id, store_id, first_name, last_name, email, address_id, activebool, create_date, last_update, active) FROM '3055.dat';

--
-- Data for Name: film; Type: TABLE DATA; Schema: dvdrental; 
--

\COPY dvdrental.film (film_id, title, description, release_year, language_id, rental_duration, rental_rate, length, replacement_cost, rating, last_update, special_features, fulltext) FROM '3061.dat';

--
-- Data for Name: film_actor; Type: TABLE DATA; Schema: dvdrental; 
--

\COPY dvdrental.film_actor (actor_id, film_id, last_update) FROM '3062.dat';

--
-- Data for Name: film_category; Type: TABLE DATA; Schema: dvdrental; 
--

\COPY dvdrental.film_category (film_id, category_id, last_update) FROM '3063.dat';

--
-- Data for Name: inventory; Type: TABLE DATA; Schema: dvdrental; 
--

\COPY dvdrental.inventory (inventory_id, film_id, store_id, last_update) FROM '3071.dat';

--
-- Data for Name: language; Type: TABLE DATA; Schema: dvdrental; 
--

\COPY dvdrental.language (language_id, name, last_update) FROM '3073.dat';

--
-- Data for Name: payment; Type: TABLE DATA; Schema: dvdrental; 
--

\COPY dvdrental.payment (payment_id, customer_id, staff_id, rental_id, amount, payment_date) FROM '3075.dat';

--
-- Data for Name: rental; Type: TABLE DATA; Schema: dvdrental; 
--

\COPY dvdrental.rental (rental_id, rental_date, inventory_id, customer_id, return_date, staff_id, last_update) FROM '3077.dat';

--
-- Data for Name: staff; Type: TABLE DATA; Schema: dvdrental; 
--

\COPY dvdrental.staff (staff_id, first_name, last_name, address_id, email, store_id, active, username, password, last_update, picture) FROM '3079.dat';

--
-- Data for Name: store; Type: TABLE DATA; Schema: dvdrental; 
--

\COPY dvdrental.store (store_id, manager_staff_id, address_id, last_update) FROM '3081.dat';

--
-- Name: actor_actor_id_seq; Type: SEQUENCE SET; Schema: dvdrental; 
--

SELECT pg_catalog.setval('dvdrental.actor_actor_id_seq', 200, true);


--
-- Name: address_address_id_seq; Type: SEQUENCE SET; Schema: dvdrental; 
--

SELECT pg_catalog.setval('dvdrental.address_address_id_seq', 605, true);


--
-- Name: category_category_id_seq; Type: SEQUENCE SET; Schema: dvdrental; 
--

SELECT pg_catalog.setval('dvdrental.category_category_id_seq', 16, true);


--
-- Name: city_city_id_seq; Type: SEQUENCE SET; Schema: dvdrental; 
--

SELECT pg_catalog.setval('dvdrental.city_city_id_seq', 600, true);


--
-- Name: country_country_id_seq; Type: SEQUENCE SET; Schema: dvdrental; 
--

SELECT pg_catalog.setval('dvdrental.country_country_id_seq', 109, true);


--
-- Name: customer_customer_id_seq; Type: SEQUENCE SET; Schema: dvdrental; 
--

SELECT pg_catalog.setval('dvdrental.customer_customer_id_seq', 599, true);


--
-- Name: film_film_id_seq; Type: SEQUENCE SET; Schema: dvdrental; 
--

SELECT pg_catalog.setval('dvdrental.film_film_id_seq', 1000, true);


--
-- Name: inventory_inventory_id_seq; Type: SEQUENCE SET; Schema: dvdrental; 
--

SELECT pg_catalog.setval('dvdrental.inventory_inventory_id_seq', 4581, true);


--
-- Name: language_language_id_seq; Type: SEQUENCE SET; Schema: dvdrental; 
--

SELECT pg_catalog.setval('dvdrental.language_language_id_seq', 6, true);


--
-- Name: payment_payment_id_seq; Type: SEQUENCE SET; Schema: dvdrental; 
--

SELECT pg_catalog.setval('dvdrental.payment_payment_id_seq', 32098, true);


--
-- Name: rental_rental_id_seq; Type: SEQUENCE SET; Schema: dvdrental; 
--

SELECT pg_catalog.setval('dvdrental.rental_rental_id_seq', 16049, true);


--
-- Name: staff_staff_id_seq; Type: SEQUENCE SET; Schema: dvdrental; 
--

SELECT pg_catalog.setval('dvdrental.staff_staff_id_seq', 2, true);


--
-- Name: store_store_id_seq; Type: SEQUENCE SET; Schema: dvdrental; 
--

SELECT pg_catalog.setval('dvdrental.store_store_id_seq', 2, true);


--
-- Name: actor actor_pkey; Type: CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.actor
    ADD CONSTRAINT actor_pkey PRIMARY KEY (actor_id);


--
-- Name: address address_pkey; Type: CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.address
    ADD CONSTRAINT address_pkey PRIMARY KEY (address_id);


--
-- Name: category category_pkey; Type: CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.category
    ADD CONSTRAINT category_pkey PRIMARY KEY (category_id);


--
-- Name: city city_pkey; Type: CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.city
    ADD CONSTRAINT city_pkey PRIMARY KEY (city_id);


--
-- Name: country country_pkey; Type: CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.country
    ADD CONSTRAINT country_pkey PRIMARY KEY (country_id);


--
-- Name: customer customer_pkey; Type: CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.customer
    ADD CONSTRAINT customer_pkey PRIMARY KEY (customer_id);


--
-- Name: film_actor film_actor_pkey; Type: CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.film_actor
    ADD CONSTRAINT film_actor_pkey PRIMARY KEY (actor_id, film_id);


--
-- Name: film_category film_category_pkey; Type: CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.film_category
    ADD CONSTRAINT film_category_pkey PRIMARY KEY (film_id, category_id);


--
-- Name: film film_pkey; Type: CONSTRAINT; Schema: dvdrental;
--

ALTER TABLE ONLY dvdrental.film
    ADD CONSTRAINT film_pkey PRIMARY KEY (film_id);


--
-- Name: inventory inventory_pkey; Type: CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.inventory
    ADD CONSTRAINT inventory_pkey PRIMARY KEY (inventory_id);


--
-- Name: language language_pkey; Type: CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.language
    ADD CONSTRAINT language_pkey PRIMARY KEY (language_id);


--
-- Name: payment payment_pkey; Type: CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.payment
    ADD CONSTRAINT payment_pkey PRIMARY KEY (payment_id);


--
-- Name: rental rental_pkey; Type: CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.rental
    ADD CONSTRAINT rental_pkey PRIMARY KEY (rental_id);


--
-- Name: staff staff_pkey; Type: CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.staff
    ADD CONSTRAINT staff_pkey PRIMARY KEY (staff_id);


--
-- Name: store store_pkey; Type: CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.store
    ADD CONSTRAINT store_pkey PRIMARY KEY (store_id);


--
-- Name: film_fulltext_idx; Type: INDEX; Schema: dvdrental; 
--

CREATE INDEX film_fulltext_idx ON dvdrental.film USING gist (fulltext);


--
-- Name: idx_actor_last_name; Type: INDEX; Schema: dvdrental; 
--

CREATE INDEX idx_actor_last_name ON dvdrental.actor USING btree (last_name);


--
-- Name: idx_fk_address_id; Type: INDEX; Schema: dvdrental; 
--

CREATE INDEX idx_fk_address_id ON dvdrental.customer USING btree (address_id);


--
-- Name: idx_fk_city_id; Type: INDEX; Schema: dvdrental; 
--

CREATE INDEX idx_fk_city_id ON dvdrental.address USING btree (city_id);


--
-- Name: idx_fk_country_id; Type: INDEX; Schema: dvdrental; 
--

CREATE INDEX idx_fk_country_id ON dvdrental.city USING btree (country_id);


--
-- Name: idx_fk_customer_id; Type: INDEX; Schema: dvdrental; 
--

CREATE INDEX idx_fk_customer_id ON dvdrental.payment USING btree (customer_id);


--
-- Name: idx_fk_film_id; Type: INDEX; Schema: dvdrental; 
--

CREATE INDEX idx_fk_film_id ON dvdrental.film_actor USING btree (film_id);


--
-- Name: idx_fk_inventory_id; Type: INDEX; Schema: dvdrental; 
--

CREATE INDEX idx_fk_inventory_id ON dvdrental.rental USING btree (inventory_id);


--
-- Name: idx_fk_language_id; Type: INDEX; Schema: dvdrental; 
--

CREATE INDEX idx_fk_language_id ON dvdrental.film USING btree (language_id);


--
-- Name: idx_fk_rental_id; Type: INDEX; Schema: dvdrental; 
--

CREATE INDEX idx_fk_rental_id ON dvdrental.payment USING btree (rental_id);


--
-- Name: idx_fk_staff_id; Type: INDEX; Schema: dvdrental; 
--

CREATE INDEX idx_fk_staff_id ON dvdrental.payment USING btree (staff_id);


--
-- Name: idx_fk_store_id; Type: INDEX; Schema: dvdrental; 
--

CREATE INDEX idx_fk_store_id ON dvdrental.customer USING btree (store_id);


--
-- Name: idx_last_name; Type: INDEX; Schema: dvdrental; 
--

CREATE INDEX idx_last_name ON dvdrental.customer USING btree (last_name);


--
-- Name: idx_store_id_film_id; Type: INDEX; Schema: dvdrental; 
--

CREATE INDEX idx_store_id_film_id ON dvdrental.inventory USING btree (store_id, film_id);


--
-- Name: idx_title; Type: INDEX; Schema: dvdrental; 
--

CREATE INDEX idx_title ON dvdrental.film USING btree (title);


--
-- Name: idx_unq_manager_staff_id; Type: INDEX; Schema: dvdrental; 
--

CREATE UNIQUE INDEX idx_unq_manager_staff_id ON dvdrental.store USING btree (manager_staff_id);


--
-- Name: idx_unq_rental_rental_date_inventory_id_customer_id; Type: INDEX; Schema: dvdrental; 
--

CREATE UNIQUE INDEX idx_unq_rental_rental_date_inventory_id_customer_id ON dvdrental.rental USING btree (rental_date, inventory_id, customer_id);


--
-- Name: film film_fulltext_trigger; Type: TRIGGER; Schema: dvdrental; 
--

CREATE TRIGGER film_fulltext_trigger BEFORE INSERT OR UPDATE ON dvdrental.film FOR EACH ROW EXECUTE PROCEDURE tsvector_update_trigger('fulltext', 'pg_catalog.english', 'title', 'description');


--
-- Name: actor last_updated; Type: TRIGGER; Schema: dvdrental; 
--

CREATE TRIGGER last_updated BEFORE UPDATE ON dvdrental.actor FOR EACH ROW EXECUTE PROCEDURE dvdrental.last_updated();


--
-- Name: address last_updated; Type: TRIGGER; Schema: dvdrental; 
--

CREATE TRIGGER last_updated BEFORE UPDATE ON dvdrental.address FOR EACH ROW EXECUTE PROCEDURE dvdrental.last_updated();


--
-- Name: category last_updated; Type: TRIGGER; Schema: dvdrental; 
--

CREATE TRIGGER last_updated BEFORE UPDATE ON dvdrental.category FOR EACH ROW EXECUTE PROCEDURE dvdrental.last_updated();


--
-- Name: city last_updated; Type: TRIGGER; Schema: dvdrental; 
--

CREATE TRIGGER last_updated BEFORE UPDATE ON dvdrental.city FOR EACH ROW EXECUTE PROCEDURE dvdrental.last_updated();


--
-- Name: country last_updated; Type: TRIGGER; Schema: dvdrental; 
--

CREATE TRIGGER last_updated BEFORE UPDATE ON dvdrental.country FOR EACH ROW EXECUTE PROCEDURE dvdrental.last_updated();


--
-- Name: customer last_updated; Type: TRIGGER; Schema: dvdrental; 
--

CREATE TRIGGER last_updated BEFORE UPDATE ON dvdrental.customer FOR EACH ROW EXECUTE PROCEDURE dvdrental.last_updated();


--
-- Name: film last_updated; Type: TRIGGER; Schema: dvdrental; 
--

CREATE TRIGGER last_updated BEFORE UPDATE ON dvdrental.film FOR EACH ROW EXECUTE PROCEDURE dvdrental.last_updated();


--
-- Name: film_actor last_updated; Type: TRIGGER; Schema: dvdrental; 
--

CREATE TRIGGER last_updated BEFORE UPDATE ON dvdrental.film_actor FOR EACH ROW EXECUTE PROCEDURE dvdrental.last_updated();


--
-- Name: film_category last_updated; Type: TRIGGER; Schema: dvdrental; 
--

CREATE TRIGGER last_updated BEFORE UPDATE ON dvdrental.film_category FOR EACH ROW EXECUTE PROCEDURE dvdrental.last_updated();


--
-- Name: inventory last_updated; Type: TRIGGER; Schema: dvdrental; 
--

CREATE TRIGGER last_updated BEFORE UPDATE ON dvdrental.inventory FOR EACH ROW EXECUTE PROCEDURE dvdrental.last_updated();


--
-- Name: language last_updated; Type: TRIGGER; Schema: dvdrental; 
--

CREATE TRIGGER last_updated BEFORE UPDATE ON dvdrental.language FOR EACH ROW EXECUTE PROCEDURE dvdrental.last_updated();


--
-- Name: rental last_updated; Type: TRIGGER; Schema: dvdrental; 
--

CREATE TRIGGER last_updated BEFORE UPDATE ON dvdrental.rental FOR EACH ROW EXECUTE PROCEDURE dvdrental.last_updated();


--
-- Name: staff last_updated; Type: TRIGGER; Schema: dvdrental; 
--

CREATE TRIGGER last_updated BEFORE UPDATE ON dvdrental.staff FOR EACH ROW EXECUTE PROCEDURE dvdrental.last_updated();


--
-- Name: store last_updated; Type: TRIGGER; Schema: dvdrental; 
--

CREATE TRIGGER last_updated BEFORE UPDATE ON dvdrental.store FOR EACH ROW EXECUTE PROCEDURE dvdrental.last_updated();


--
-- Name: customer customer_address_id_fkey; Type: FK CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.customer
    ADD CONSTRAINT customer_address_id_fkey FOREIGN KEY (address_id) REFERENCES dvdrental.address(address_id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- Name: film_actor film_actor_actor_id_fkey; Type: FK CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.film_actor
    ADD CONSTRAINT film_actor_actor_id_fkey FOREIGN KEY (actor_id) REFERENCES dvdrental.actor(actor_id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- Name: film_actor film_actor_film_id_fkey; Type: FK CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.film_actor
    ADD CONSTRAINT film_actor_film_id_fkey FOREIGN KEY (film_id) REFERENCES dvdrental.film(film_id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- Name: film_category film_category_category_id_fkey; Type: FK CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.film_category
    ADD CONSTRAINT film_category_category_id_fkey FOREIGN KEY (category_id) REFERENCES dvdrental.category(category_id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- Name: film_category film_category_film_id_fkey; Type: FK CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.film_category
    ADD CONSTRAINT film_category_film_id_fkey FOREIGN KEY (film_id) REFERENCES dvdrental.film(film_id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- Name: film film_language_id_fkey; Type: FK CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.film
    ADD CONSTRAINT film_language_id_fkey FOREIGN KEY (language_id) REFERENCES dvdrental.language(language_id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- Name: address fk_address_city; Type: FK CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.address
    ADD CONSTRAINT fk_address_city FOREIGN KEY (city_id) REFERENCES dvdrental.city(city_id);


--
-- Name: city fk_city; Type: FK CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.city
    ADD CONSTRAINT fk_city FOREIGN KEY (country_id) REFERENCES dvdrental.country(country_id);


--
-- Name: inventory inventory_film_id_fkey; Type: FK CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.inventory
    ADD CONSTRAINT inventory_film_id_fkey FOREIGN KEY (film_id) REFERENCES dvdrental.film(film_id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- Name: payment payment_customer_id_fkey; Type: FK CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.payment
    ADD CONSTRAINT payment_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES dvdrental.customer(customer_id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- Name: payment payment_rental_id_fkey; Type: FK CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.payment
    ADD CONSTRAINT payment_rental_id_fkey FOREIGN KEY (rental_id) REFERENCES dvdrental.rental(rental_id) ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: payment payment_staff_id_fkey; Type: FK CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.payment
    ADD CONSTRAINT payment_staff_id_fkey FOREIGN KEY (staff_id) REFERENCES dvdrental.staff(staff_id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- Name: rental rental_customer_id_fkey; Type: FK CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.rental
    ADD CONSTRAINT rental_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES dvdrental.customer(customer_id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- Name: rental rental_inventory_id_fkey; Type: FK CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.rental
    ADD CONSTRAINT rental_inventory_id_fkey FOREIGN KEY (inventory_id) REFERENCES dvdrental.inventory(inventory_id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- Name: rental rental_staff_id_key; Type: FK CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.rental
    ADD CONSTRAINT rental_staff_id_key FOREIGN KEY (staff_id) REFERENCES dvdrental.staff(staff_id);


--
-- Name: staff staff_address_id_fkey; Type: FK CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.staff
    ADD CONSTRAINT staff_address_id_fkey FOREIGN KEY (address_id) REFERENCES dvdrental.address(address_id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- Name: store store_address_id_fkey; Type: FK CONSTRAINT; Schema: dvdrental;
--

ALTER TABLE ONLY dvdrental.store
    ADD CONSTRAINT store_address_id_fkey FOREIGN KEY (address_id) REFERENCES dvdrental.address(address_id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- Name: store store_manager_staff_id_fkey; Type: FK CONSTRAINT; Schema: dvdrental; 
--

ALTER TABLE ONLY dvdrental.store
    ADD CONSTRAINT store_manager_staff_id_fkey FOREIGN KEY (manager_staff_id) REFERENCES dvdrental.staff(staff_id) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- PostgreSQL database dump complete
--

