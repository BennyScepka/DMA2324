DROP VIEW IF EXISTS descendant CASCADE;
DROP TABLE IF EXISTS sibling CASCADE;
CREATE TABLE Sibling ( sib1 integer, sib2 integer);

INSERT INTO sibling VALUES (1,4);
INSERT INTO sibling VALUES (5,6);
INSERT INTO sibling VALUES (2,8);

DROP VIEW IF EXISTS SIB;
CREATE VIEW SIB AS SELECT sib1,sib2 FROM sibling UNION SELECT sib2,sib1 FROM sibling;


DROP TABLE IF EXISTS childOf CASCADE;
CREATE TABLE childOf ( parent integer, child integer);

INSERT INTO childOf VALUES (1,2);
INSERT INTO childOf VALUES (1,3);
INSERT INTO childOf VALUES (5,2);
INSERT INTO childOf VALUES (6,7);
INSERT INTO childOf VALUES (2,8);

DROP TABLE IF EXISTS person CASCADE;
CREATE TABLE person ( id integer primary key, name varchar, dob date, gender char, city varchar);

INSERT INTO person VALUES(1,'James Doe','2016-04-04','m','Vienna');
INSERT INTO person VALUES(2,'John Doe','2016-04-04','m','Vienna');
INSERT INTO person VALUES(3,'Judy Doe','2016-04-04','f','Vienna');
INSERT INTO person VALUES(4,'Jane Doe','2016-04-04','f','Vienna');
INSERT INTO person VALUES(5,'Rebecca Rabbit','2016-04-04','f','Vienna');
INSERT INTO person VALUES(6,'Zoe Zebra','2016-04-04','f','Vienna');
INSERT INTO person VALUES(7,'Ziggy Zebra','2016-04-04','m','Vienna');
INSERT INTO person VALUES(8,'Joan Doe','2016-04-04','f','Vienna');

-- John's aunts:
SELECT ja.id, ja.name FROM childof c, person j, person ja, sib s WHERE
                j.name = 'John Doe' AND
                c.child = j.id AND
                c.parent = s.sib1 AND
                s.sib2 = ja.id AND
                ja.gender = 'f';

-- descendants (recursive) of James Doe:
CREATE RECURSIVE VIEW descendant(cID, hops) AS (
  SELECT child AS cID, 1 AS hops FROM childOf WHERE parent=1
 UNION
      SELECT child, hops+1 AS hops FROM descendant, childOf WHERE cID = parent
 );

SELECT * FROM descendant;
