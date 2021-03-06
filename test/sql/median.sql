CREATE TABLE intvals(val int, color text);

-- Test empty table
SELECT median(val) FROM intvals;

-- Integers with odd number of values
INSERT INTO intvals VALUES
       (1, 'a'),
       (2, 'c'),
       (9, 'b'),
       (7, 'c'),
       (2, 'd'),
       (-3, 'd'),
       (2, 'e');

SELECT * FROM intvals ORDER BY val;
SELECT median(val) FROM intvals;

-- Integers with NULLs and even number of values
INSERT INTO intvals VALUES
       (99, 'a'),
       (NULL, 'a'),
       (NULL, 'e'),
       (NULL, 'b'),
       (7, 'c'),
       (0, 'd');

SELECT * FROM intvals ORDER BY val;
SELECT median(val) FROM intvals;

-- Text values
CREATE TABLE textvals(val text, color int);

INSERT INTO textvals VALUES
       ('erik', 1),
       ('mat', 3),
       ('rob', 8),
       ('david', 9),
       ('lee', 2);

SELECT * FROM textvals ORDER BY val;
SELECT median(val) FROM textvals;

-- Test large table with timestamps
CREATE TABLE timestampvals (val timestamptz);

INSERT INTO timestampvals(val)
SELECT TIMESTAMP 'epoch' + (i * INTERVAL '1 second')
FROM generate_series(0, 100000) as T(i);

SELECT median(val) FROM timestampvals;

--test even number of values
CREATE TABLE evenvals(val int, color text, magnitude float);
INSERT INTO evenvals VALUES
       (10, 'a', 1.0),
       (9, 'b', 5.0),
       (8, 'c', 2.0),
       (7, 'd', 7.0),
       (5, 'e', 3.0),
       (4, 'f', 4.0),
       (3, 'g', 6.0),
       (2, 'h', 8.0);

SELECT * FROM evenvals ORDER BY val;
SELECT median(val) FROM evenvals;

SELECT * FROM evenvals ORDER BY color;
SELECT median(color) FROM evenvals;

SELECT * FROM evenvals ORDER BY magnitude;
SELECT median(magnitude) FROM evenvals;

-- SELECT * FROM evenvals ORDER BY (val, color, magnitude);
-- TODO the following doesn't work, should it?
-- SELECT median((val, color, magnitude)) FROM evenvals;

--Test windowing, the following two should have the same values in reverse order
SELECT median(val) OVER (ORDER BY val DESC) FROM evenvals;
SELECT median(val)
    OVER (ORDER BY val ROWS between CURRENT ROW AND UNBOUNDED FOLLOWING)
    FROM evenvals;

SELECT median(val) 
    OVER (ORDER BY val ROWS between CURRENT ROW AND 2 FOLLOWING)
    FROM evenvals;

--test all the same value
CREATE TABLE samevals(val int);

INSERT INTO samevals VALUES
       (72);

SELECT * FROM samevals ORDER BY val;
SELECT median(val) FROM evenvals;

INSERT INTO samevals VALUES
       (72);

SELECT * FROM samevals ORDER BY val;
SELECT median(val) FROM samevals;

INSERT INTO samevals SELECT 72 FROM generate_series(1,100000);

SELECT median(val) FROM samevals;
