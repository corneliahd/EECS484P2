package project2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MyFakebookOracle extends FakebookOracle {

    static String prefix = "SYZHAO.";

    // You must use the following variable as the JDBC connection
    Connection oracleConnection = null;

    // You must refer to the following variables for the corresponding tables in your database
    String cityTableName = null;
    String userTableName = null;
    String friendsTableName = null;
    String currentCityTableName = null;
    String hometownCityTableName = null;
    String programTableName = null;
    String educationTableName = null;
    String eventTableName = null;
    String participantTableName = null;
    String albumTableName = null;
    String photoTableName = null;
    String coverPhotoTableName = null;
    String tagTableName = null;


    // DO NOT modify this constructor
    public MyFakebookOracle(String dataType, Connection c) {
        super();
        oracleConnection = c;
        // You will use the following tables in your Java code
        cityTableName = prefix + dataType + "_CITIES";
        userTableName = prefix + dataType + "_USERS";
        friendsTableName = prefix + dataType + "_FRIENDS";
        currentCityTableName = prefix + dataType + "_USER_CURRENT_CITY";
        hometownCityTableName = prefix + dataType + "_USER_HOMETOWN_CITY";
        programTableName = prefix + dataType + "_PROGRAMS";
        educationTableName = prefix + dataType + "_EDUCATION";
        eventTableName = prefix + dataType + "_USER_EVENTS";
        albumTableName = prefix + dataType + "_ALBUMS";
        photoTableName = prefix + dataType + "_PHOTOS";
        tagTableName = prefix + dataType + "_TAGS";
    }


    @Override
    // ***** Query 0 *****
    // This query is given to your for free;
    // You can use it as an example to help you write your own code
    //
    public void findMonthOfBirthInfo() {

        // Scrollable result set allows us to read forward (using next())
        // and also backward.
        // This is needed here to support the user of isFirst() and isLast() methods,
        // but in many cases you will not need it.
        // To create a "normal" (unscrollable) statement, you would simply call
        // Statement stmt = oracleConnection.createStatement();
        //
        try (Statement stmt =
                     oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                             ResultSet.CONCUR_READ_ONLY)) {

            // For each month, find the number of users born that month
            // Sort them in descending order of count
            ResultSet rst = stmt.executeQuery("select count(*), month_of_birth from " +
                    userTableName +
                    " where month_of_birth is not null group by month_of_birth order by 1 desc");

            this.monthOfMostUsers = 0;
            this.monthOfLeastUsers = 0;
            this.totalUsersWithMonthOfBirth = 0;

            // Get the month with most users, and the month with least users.
            // (Notice that this only considers months for which the number of users is > 0)
            // Also, count how many total users have listed month of birth (i.e., month_of_birth not null)
            //
            while (rst.next()) {
                int count = rst.getInt(1);
                int month = rst.getInt(2);
                if (rst.isFirst())
                    this.monthOfMostUsers = month;
                if (rst.isLast())
                    this.monthOfLeastUsers = month;
                this.totalUsersWithMonthOfBirth += count;
            }

            // Get the names of users born in the "most" month
            rst = stmt.executeQuery("select user_id, first_name, last_name from " +
                    userTableName + " where month_of_birth=" + this.monthOfMostUsers);
            while (rst.next()) {
                Long uid = rst.getLong(1);
                String firstName = rst.getString(2);
                String lastName = rst.getString(3);
                this.usersInMonthOfMost.add(new UserInfo(uid, firstName, lastName));
            }

            // Get the names of users born in the "least" month
            rst = stmt.executeQuery("select first_name, last_name, user_id from " +
                    userTableName + " where month_of_birth=" + this.monthOfLeastUsers);
            while (rst.next()) {
                String firstName = rst.getString(1);
                String lastName = rst.getString(2);
                Long uid = rst.getLong(3);
                this.usersInMonthOfLeast.add(new UserInfo(uid, firstName, lastName));
            }

            // Close statement and result set
            rst.close();
            stmt.close();
        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
    }

    @Override
    // ***** Query 1 *****
    // Find information about users' names:
    // (1) The longest first name (if there is a tie, include all in result)
    // (2) The shortest first name (if there is a tie, include all in result)
    // (3) The most common first name, and the number of times it appears (if there
    //      is a tie, include all in result)
    //
    public void findNameInfo() { // Query1

        try (Statement stmt =
                     oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                             ResultSet.CONCUR_READ_ONLY)) {
            //longest and shortest firstname
            ResultSet rst = stmt.executeQuery("SELECT U.first_name, LENGTH(U.first_name) as lengthofFirstname FROM " 
                + userTableName + " U GROUP BY U.first_name ORDER BY LengthofFirstname DESC");

            int max = 0;

            rst.afterLast();
            rst.previous();
            int min = rst.getInt(2);

            rst.beforeFirst();
            while (rst.next()) {
                String firstname = rst.getString(1);
                int length = rst.getInt(2);
                if (rst.isFirst()){
                    max = length;
                    this.longestFirstNames.add(firstname);
                }
                else{
                    if(length == max)
                        this.longestFirstNames.add(firstname);
                    if(length == min)
                        this.shortestFirstNames.add(firstname);
                }
            }

            this.mostCommonFirstNamesCount = 0;

            // Get the names of users born in the "most" month
            rst = stmt.executeQuery("SELECT U.first_name, COUNT(*) as numofName FROM "
                + userTableName +" U GROUP BY U.first_name ORDER BY numofName DESC");

            int maxi = 0;
            while (rst.next()) {
                String firstname = rst.getString(1);
                int length = rst.getInt(2);
                if (rst.isFirst()){
                    maxi = length;
                    this.mostCommonFirstNamesCount = maxi;
                }
                else{
                    if(length != maxi)
                        break;
                }
                this.mostCommonFirstNames.add(firstname);
            }
            // Close statement and result set
            rst.close();
            stmt.close();
        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }



    }

    @Override
    // ***** Query 2 *****
    // Find the user(s) who have no friends in the network
    //
    // Be careful on this query!
    // Remember that if two users are friends, the friends table
    // only contains the pair of user ids once, subject to
    // the constraint that user1_id < user2_id
    //
    public void lonelyUsers() {
        // Find the following information from your database and store the information as shown
        //this.lonelyUsers.add(new UserInfo(10L, "Billy", "SmellsFunny"));
        //this.lonelyUsers.add(new UserInfo(11L, "Jenny", "BadBreath"));
        try (Statement stmt = oracleConnection.createStatement()) 
        {
            ResultSet rst = stmt.executeQuery("SELECT USER_ID, FIRST_NAME, LAST_NAME FROM " + 
                userTableName + " U WHERE USER_ID = ANY(SELECT DISTINCT U1.user_id FROM " + 
                userTableName + " U1 MINUS SELECT DISTINCT F1.user1_id FROM " + 
                friendsTableName + " F1 MINUS SELECT DISTINCT F2.user2_id FROM " + 
                friendsTableName + " F2)");

                while (rst.next()) 
                {
                Long uid = rst.getLong(1);
                String firstName = rst.getString(2);
                String lastName = rst.getString(3);
                this.lonelyUsers.add(new UserInfo(uid, firstName, lastName));
                }
            rst.close();
            stmt.close(); 
        }

        catch (SQLException err) 
        {
            System.err.println(err.getMessage());
        }
    }

    @Override
    // ***** Query 3 *****
    // Find the users who do not live in their hometowns
    // (I.e., current_city != hometown_city)
    //
    public void liveAwayFromHome() throws SQLException {
        try (Statement stmt =
            oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
                   
            ResultSet rst = stmt.executeQuery("SELECT U.user_id, U.first_name, U.last_name FROM "
                + userTableName +" U, "+ currentCityTableName +" C2, "
                + hometownCityTableName +" C1 WHERE U.user_id = C1.user_id AND U.user_id = C2.user_id AND C1.hometown_city_id <> C2.current_city_id ORDER BY U.user_id");
            
            while (rst.next()){
                Long uid = rst.getLong(1);
                String firstname = rst.getString(2);
                String lastname = rst.getString(3);
                this.liveAwayFromHome.add(new UserInfo(uid, firstname, lastname));
            }
            rst.close();
            stmt.close(); 

        } catch (SQLException err) {
                System.err.println(err.getMessage());
        }
    }

    @Override
    // **** Query 4 ****
    // Find the top-n photos based on the number of tagged users
    // If there are ties, choose the photo with the smaller numeric PhotoID first
    //
    public void findPhotosWithMostTags(int n) {

        try (Statement stmt =
            oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {

            ResultSet rst = stmt.executeQuery("SELECT DISTINCT T.TAG_PHOTO_ID, A.ALBUM_ID, A.ALBUM_NAME, P.PHOTO_CAPTION, P.PHOTO_LINK FROM "
                + tagTableName +" T, " + photoTableName + " P, " 
                + albumTableName + " A WHERE P.PHOTO_ID = T.TAG_PHOTO_ID AND P.ALBUM_ID = A.ALBUM_ID AND T.TAG_PHOTO_ID = ANY( SELECT DISTINCT TAG_PHOTO_ID FROM( SELECT DISTINCT T.TAG_PHOTO_ID, COUNT(T.TAG_PHOTO_ID) AS TAGNUM FROM "
                + tagTableName +" T GROUP BY T.TAG_PHOTO_ID ORDER BY TAGNUM DESC, T.TAG_PHOTO_ID ASC ) WHERE ROWNUM <= " + n + ") ORDER BY T.TAG_PHOTO_ID ASC");
            
            while (rst.next()){
                String photoId = rst.getString(1);
                String albumId = rst.getString(2);
                String albumName = rst.getString(3);
                String photoCaption = rst.getString(4);
                String photoLink = rst.getString(5);
                System.out.println(photoId);
                PhotoInfo p = new PhotoInfo(photoId, albumId, albumName, photoCaption, photoLink);
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                Statement stm = oracleConnection.createStatement();
                ResultSet rs = stm.executeQuery("SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME FROM "
                    + tagTableName +" T, "+ userTableName +" U WHERE T.TAG_PHOTO_ID = '"+ photoId +"' AND T.TAG_SUBJECT_ID = U.USER_ID");
                while (rs.next()){
                    Long uid = rs.getLong(1);
                    String firstname = rs.getString(2);
                    String lastname = rs.getString(3);
                    tp.addTaggedUser(new UserInfo(uid, firstname, lastname));
                }
                this.photosWithMostTags.add(tp);
                rs.close();
                stm.close(); 
            }
            rst.close();
            stmt.close(); 

        } catch (SQLException err) {
                System.err.println(err.getMessage());
        }
    }

    @Override
    // **** Query 5 ****
    // Find suggested "match pairs" of users, using the following criteria:
    // (1) Both users should be of the same gender
    // (2) They should be tagged together in at least one photo (They do not have to be friends of the same person)
    // (3) Their age difference is <= yearDiff (just compare the years of birth for this)
    // (4) They are not friends with one another
    //
    // You should return up to n "match pairs"
    // If there are more than n match pairs, you should break ties as followsU:
    // (i) First choose the pairs with the largest number of shared photos
    // (ii) If there are still ties, choose the pair with the smaller user1_id
    // (iii) If there are still ties, choose the pair with the smaller user2_id
    //
    public void matchMaker(int n, int yearDiff) {

        try (Statement stmt = oracleConnection.createStatement()) 
        {
            ResultSet rst = stmt.executeQuery("SELECT U1.USER_ID, U1.FIRST_NAME, U1.LAST_NAME, U1.YEAR_OF_BIRTH, U2.USER_ID, U2.FIRST_NAME, U2.LAST_NAME, U2.YEAR_OF_BIRTH FROM " + 
                userTableName + " U1," + 
                userTableName + " U2, (SELECT ID1, ID2 FROM(SELECT T1.TAG_SUBJECT_ID AS ID1, T2.TAG_SUBJECT_ID AS ID2 FROM " + 
                tagTableName + " T1," + 
                tagTableName + " T2 WHERE T1.TAG_SUBJECT_ID < T2.TAG_SUBJECT_ID AND T1.TAG_PHOTO_ID = T2.TAG_PHOTO_ID MINUS SELECT F.USER1_ID, F.USER2_ID FROM " + 
                friendsTableName + " F) GROUP BY ID1, ID2 ORDER BY ID1 ASC, ID2 ASC)A WHERE A.ID1 = U1.USER_ID AND A.ID2 = U2.USER_ID AND U1.GENDER = U2.GENDER AND ABS(U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH) <= " + 
                yearDiff + " AND ROWNUM <= " + n);

            while (rst.next()) 
            {
                Long u1UserId = rst.getLong(1);
                String u1FirstName = rst.getString(2);
                String u1LastName = rst.getString(3);
                int u1Year = rst.getInt(4);
                Long u2UserId = rst.getLong(5);
                String u2FirstName = rst.getString(6);
                String u2LastName = rst.getString(7);
                int u2Year = rst.getInt(8);
                MatchPair mp = new MatchPair(u1UserId, u1FirstName, u1LastName, u1Year, u2UserId, u2FirstName, u2LastName, u2Year);

                Statement stm = oracleConnection.createStatement();
                ResultSet rs = stm.executeQuery("SELECT P.PHOTO_ID, P.ALBUM_ID, A.ALBUM_NAME, P.PHOTO_CAPTION, P.PHOTO_LINK FROM " + 
                    photoTableName + " P," + 
                    tagTableName + " T1," + 
                    tagTableName + " T2," +
                    albumTableName + " A WHERE T1.TAG_SUBJECT_ID = " + 
                    u1UserId + " AND T2.TAG_SUBJECT_ID = " + 
                    u2UserId + " AND T1.TAG_PHOTO_ID = T2.TAG_PHOTO_ID AND T1.TAG_PHOTO_ID = P.PHOTO_ID AND P.ALBUM_ID = A.ALBUM_ID");
                while (rs.next()) 
                {
                    String sharedPhotoId = rst.getString(1);
                    String sharedPhotoAlbumId = rst.getString(2);
                    String sharedPhotoAlbumName = rst.getString(3);
                    String sharedPhotoCaption = rst.getString(4);
                    String sharedPhotoLink = rst.getString(5);
                    mp.addSharedPhoto(new PhotoInfo(sharedPhotoId, sharedPhotoAlbumId, sharedPhotoAlbumName, sharedPhotoCaption, sharedPhotoLink));
                }
                this.bestMatches.add(mp);
                rs.close();
                stm.close();

            }
            rst.close();
            stmt.close();

        } 
        catch (SQLException err) 
        {
            System.err.println(err.getMessage());
        }
    }

    // **** Query 6 ****
    // Suggest users based on mutual friends
    //
    // Find the top n pairs of users in the database who have the most
    // common friends, but are not friends themselves.
    //
    // Your output will consist of a set of pairs (user1_id, user2_id)
    // No pair should appear in the result twice; you should always order the pairs so that
    // user1_id < user2_id
    //
    // If there are ties, you should give priority to the pair with the smaller user1_id.
    // If there are still ties, give priority to the pair with the smaller user2_id.
    //
    @Override

    public void suggestFriendsByMutualFriends(int n) {

        try (Statement stmt = oracleConnection.createStatement()) 
        {
            ResultSet rst = stmt.executeQuery("SELECT DISTINCT A.UID1, U1.FIRST_NAME, U1.LAST_NAME, A.UID2, U2.FIRST_NAME, U2.LAST_NAME, A.CommonFri FROM " + 
                userTableName + " U1, " + 
                userTableName + " U2, ( SELECT N.UID1, N.UID2, COUNT(*) AS CommonFri FROM ( SELECT F.USER1_ID AS ID1, F.USER2_ID AS ID2 FROM " +
                friendsTableName + " F UNION SELECT F.USER2_ID AS ID1, F.USER1_ID AS ID2 FROM " +
                friendsTableName + " F )F1, ( SELECT F.USER1_ID AS ID1, F.USER2_ID AS ID2 FROM " +
                friendsTableName + " F UNION SELECT F.USER2_ID AS ID1, F.USER1_ID AS ID2 FROM " +
                friendsTableName + " F )F2, ( SELECT U1.USER_ID AS UID1, U2.USER_ID AS UID2 FROM " +
                userTableName + " U1, " + 
                userTableName + " U2 WHERE U1.USER_ID < U2.USER_ID MINUS SELECT F.USER1_ID AS UID1, F.USER2_ID AS UID2 FROM " +
                friendsTableName + " F )N WHERE N.UID1 = F1.ID1 AND N.UID2 = F2.ID1 AND F1.ID2 = F2.ID2 GROUP BY N.UID1, N.UID2 ORDER BY CommonFri DESC, N.UID1 ASC, N.UID2 ASC )A WHERE U1.USER_ID = A.UID1 AND U2.USER_ID = A.UID2 AND ROWNUM <= " +
                n + " ORDER BY A.CommonFri DESC, A.UID1 ASC, A.UID2 ASC");

            while (rst.next()) 
            {
                Long user1_id = rst.getLong(1);
                String user1FirstName = rst.getString(2);
                String user1LastName = rst.getString(3);
                Long user2_id = rst.getLong(4);
                String user2FirstName = rst.getString(5);
                String user2LastName = rst.getString(6);
                UsersPair p = new UsersPair(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);

                Statement st = oracleConnection.createStatement();
                ResultSet r = st.executeQuery("SELECT A.ID, U.FIRST_NAME, U.LAST_NAME FROM " +
                    userTableName + " U, (((SELECT USER2_ID AS ID FROM " + 
                    friendsTableName + " F WHERE F.USER1_ID = " + user1_id + ") UNION (SELECT USER1_ID AS ID FROM " +
                    friendsTableName + " F WHERE F.USER2_ID = " + user1_id + ")) INTERSECT ((SELECT USER2_ID AS ID FROM " +
                    friendsTableName + " F WHERE F.USER1_ID = " + user2_id + ") UNION (SELECT USER1_ID AS ID FROM " +
                    friendsTableName + " F WHERE F.USER2_ID = " + user2_id + ")))A WHERE U.USER_ID = A.ID ORDER BY A.ID");

                while(r.next())
                { 
                    Long uid = r.getLong(1);
                    String firstName = r.getString(2);
                    String lastName = r.getString(3);
                    p.addSharedFriend(uid, firstName, lastName);
                    
                }
                this.suggestedUsersPairs.add(p);
                r.close();
                st.close();
            }
            rst.close();
            stmt.close();
        } 
        catch (SQLException err) 
        {
            System.err.println(err.getMessage());
        }


    }

    @Override
    // ***** Query 7 *****
    //
    // Find the name of the state with the most events, as well as the number of
    // events in that state.  If there is a tie, return the names of all of the (tied) states.
    //
    public void findEventStates() {
        this.eventCount = 0;

        try (Statement stmt = oracleConnection.createStatement()) 
        {
            ResultSet rst = stmt.executeQuery("SELECT C.STATE_NAME, COUNT(*) AS EVENT_NUM FROM " + 
                eventTableName + " E," + cityTableName + " C WHERE E.EVENT_CITY_ID = C.CITY_ID GROUP BY C.STATE_NAME ORDER BY EVENT_NUM DESC");
            
            rst.next();
            String stateName = rst.getString(1);
            int count = rst.getInt(2);
            int maxi = count;
            this.eventCount = count;
            this.popularStateNames.add(stateName);

            while (rst.next()) {
                stateName = rst.getString(1);
                count = rst.getInt(2);
                if(count != maxi)
                    break;
                this.eventCount = count;
                this.popularStateNames.add(stateName);
            }
            rst.close();
            stmt.close(); 
        } 
        catch (SQLException err) 
        {
            System.err.println(err.getMessage());
        }
    }

    //@Override
    // ***** Query 8 *****
    // Given the ID of a user, find information about that
    // user's oldest friend and youngest friend
    //
    // If two users have exactly the same age, meaning that they were born
    // on the same day, then assume that the one with the larger user_id is older
    //
    public void findAgeInfo(Long user_id) {
        //this.oldestFriend = new UserInfo(1L, "Oliver", "Oldham");
        //this.youngestFriend = new UserInfo(25L, "Yolanda", "Young");
        try (Statement stmt =
                     oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                             ResultSet.CONCUR_READ_ONLY)) 
        {
            ResultSet rst = stmt.executeQuery("SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME FROM " + 
                userTableName + " U, (SELECT F.USER2_ID AS USER_ID FROM " + 
                friendsTableName + " F WHERE F.USER1_ID = " + 
                user_id + " UNION SELECT F.USER1_ID AS USER_ID FROM " + 
                friendsTableName + " F WHERE F.USER2_ID = " + 
                user_id + " )F WHERE F.USER_ID = U.USER_ID ORDER BY U.YEAR_OF_BIRTH DESC, U.MONTH_OF_BIRTH DESC, U.DAY_OF_BIRTH DESC, U.USER_ID ASC");

            while (rst.next()) 
            {
                Long uid = rst.getLong(1);
                String firstName = rst.getString(2);
                String lastName = rst.getString(3);
                if (rst.isFirst()) 
                    this.youngestFriend = new UserInfo(uid, firstName, lastName);
                if (rst.isLast()) 
                    this.oldestFriend = new UserInfo(uid, firstName, lastName);
            }
            rst.close();
            stmt.close(); 

        } 
        catch (SQLException err) 
        {
            System.err.println(err.getMessage());
        }
    }

    @Override
    //	 ***** Query 9 *****
    //
    // Find pairs of potential siblings.
    //
    // A pair of users are potential siblings if they have the same last name and hometown, if they are friends, and
    // if they are less than 10 years apart in age.  Pairs of siblings are returned with the lower user_id user first
    // on the line.  They are ordered based on the first user_id and in the event of a tie, the second user_id.
    //
    //
    public void findPotentialSiblings() {

        try (Statement stmt = oracleConnection.createStatement()) 
        {
            ResultSet rst = stmt.executeQuery("SELECT U1.USER_ID AS USER1_ID, U1.FIRST_NAME, U1.LAST_NAME, U2.USER_ID AS USER2_ID, U2.FIRST_NAME, U2.LAST_NAME FROM " +
                userTableName + " U1," +
                userTableName + " U2," + 
                hometownCityTableName + " H1," +
                hometownCityTableName + " H2," +
                friendsTableName + " F WHERE U1.USER_ID = H1.USER_ID AND U2.USER_ID = H2.USER_ID AND H1.HOMETOWN_CITY_ID = H2.HOMETOWN_CITY_ID AND U1.USER_ID < U2.USER_ID AND U1.USER_ID = F.USER1_ID AND F.USER2_ID = U2.USER_ID AND ABS(U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH) < 10 AND U1.LAST_NAME = U2.LAST_NAME ORDER BY USER1_ID ASC, USER2_ID ASC");

            while (rst.next()) 
            {
                Long user1_id = rst.getLong(1);
                String user1FirstName = rst.getString(2);
                String user1LastName = rst.getString(3);
                Long user2_id = rst.getLong(4);
                String user2FirstName = rst.getString(5);
                String user2LastName = rst.getString(6);
                SiblingInfo s = new SiblingInfo(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);
                this.siblings.add(s);
            }
            rst.close();
            stmt.close(); 
        }
        catch (SQLException err) 
        {
            System.err.println(err.getMessage());
        }
    }

}
