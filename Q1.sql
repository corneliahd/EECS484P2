
SELECT U.first_name, COUNT(*) as numofName, LEN(U.first_name) as lengthofFirstname
FROM USERS U
GROUP BY U.first_name
ORDER BY LengthofFirstname DESC




SELECT U.first_name, LEN(U.first_name) as lengthofFirstname
FROM USERS U
GROUP BY U.first_name
ORDER BY LengthofFirstname DESC

SELECT U.first_name, COUNT(*) as numofName
FROM USERS U
GROUP BY U.first_name
ORDER BY numofName DESC