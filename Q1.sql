SELECT U.first_name, LENGTH(U.first_name) as lengthofFirstname
FROM USER1 U
GROUP BY U.first_name
ORDER BY LengthofFirstname DESC;

SELECT U.first_name, COUNT(*) as numofName
FROM USER1 U
GROUP BY U.first_name
ORDER BY numofName DESC;
