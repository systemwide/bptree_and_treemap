/*****************************************************************************************
 * @file  MovieDB.java
 *
 * @author   John Miller
 */

import static java.lang.System.out;

import java.util.Random;
// for help with random number generator


/*****************************************************************************************
 * The MovieDB class makes a Movie Database.  It serves as a template for making other
 * databases.  See "Database Systems: The Complete Book", second edition, page 26 for more
 * information on the Movie Database schema.
 */
class MovieDB
{
    /*************************************************************************************
	
     * Main method for creating, populating and querying a Movie Database.
     * @param args  the command-line arguments
     */
    public static void main (String [] args)
    {
        out.println ();

        Table movie = new Table ("movie", "title year length genre studioName producerNo",
                                          "String Integer Integer String String Integer", "title year");

        Table cinema = new Table ("cinema", "title year length genre studioName producerNo",
                                            "String Integer Integer String String Integer", "title year");

        Table movieStar = new Table ("movieStar", "name address gender birthdate",
                                                  "String String Character String", "name");

        Table starsIn = new Table ("starsIn", "movieTitle movieYear starName",
                                              "String Integer String", "movieTitle movieYear starName");

        Table movieExec = new Table ("movieExec", "certNo name address fee",
                                                  "Integer String String Float", "certNo");

        Table studio = new Table ("studio", "name address presNo",
                                            "String String Integer", "name");
        
        
        String[] randGenres = {"Action", "SciFi", "RomCom", "Drama", "Thriller", "Horror"};
  
/********************************************
 *  Code to generate large sets of tuples for movie table
 *  @author Ben Rotolo       
 */
        Random rand = new Random();
       
        // Generate 150 tuples for movie table
        for(int i = 0;i<=150;i++)
        {
        	int randDate = rand.nextInt(97) + 1920;
        	int randLength = rand.nextInt(90) + 90;
        	int randProducer = rand.nextInt(50);
        	int randStudio = rand.nextInt(20) + 1;
        	String thisGenre = randGenres[rand.nextInt(6)];
        	
        	Comparable [] filmX = {"Film_"+ i, randDate, randLength, thisGenre, "Studio_" + randStudio, randProducer};
        	
        	// populate table movie
        	movie.insert(filmX);
        	// populate cinema with only even entries.
        	if(i%2 == 0) cinema.insert(filmX);
        }
        
        movie.print();
        out.println();
        
 /********************************************
  *  Code to generate large sets of tuples for moveStar table
  *  @author Ben Rotolo       
  */
        for(int i = 0;i<=200;i++)
        {
        	// random variables for movie star bio
        	int randMonth = rand.nextInt(12) +1;
        	int randDay = rand.nextInt(28) + 1;
        	int randYear = rand.nextInt(80) + 19;
        	int randAddress = rand.nextInt(3000);
        	char starGender;
        	// decide gender
        	if(i % 2 == 0) starGender = 'F';
        	else starGender = 'M';
        	String randBirthday = Integer.toString(randMonth) + "/" + Integer.toString(randDay) + "/" + Integer.toString(randYear);
        	
        	Comparable [] starX = {"StarName_"+i, "Hollywood", starGender, randBirthday};
        	movieStar.insert(starX);
        }

        out.println ();
        
        movieStar.print ();
        
        // Table "starsin" is not used in testing so I have not implemented it here
        
        // cast is not used in testing cases so I opted not to alter it
        Comparable [] cast0 = { "Star_Wars", 1977, "Carrie_Fisher" };
        out.println ();
        starsIn.insert (cast0);
        starsIn.print ();

        /********************************************
         *  Code to generate large sets of tuples for movieExec table
         *  @author Ben Rotolo       
         */
        
        for(int i = 0;i <= 50;i++)
        {
        	int randSalary = rand.nextInt() + 10000;
        	Comparable [] execX = { i, "Exec_" + i, "Hollywood", (float)randSalary };
        	movieExec.insert(execX);
        }
        out.println();
        movieExec.print ();

        
        /********************************************
         *  Code to generate large sets of tuples for studio table
         *  @author Ben Rotolo       
         */
        
        for(int i= 0;i<=20;i++)
        {
        	Comparable [] studioX = {"Studio_" + i, "Holloywood, CA", i * 111};
        	studio.insert(studioX);
         }
        
        
        out.println ();
        studio.print ();

        movie.save ();
        cinema.save ();
        movieStar.save ();
        starsIn.save ();
        movieExec.save ();
        studio.save ();

        movieStar.printIndex ();
        
        /**************************** BEGIN TEST CASES ****************************/
        /**
         * @author - Ben Rotolo
         * for modified test cases from original MovieDB
         */           
       
        /**
         * Select case a. Select without any where clause, i.e., no attributes specified in the method
         */
        out.println();
        out.println("TEST: SELECT CASE A");
        Table select_case_a = movie.select();
        select_case_a.print();
        select_case_a.save (); 
        
        /**
         * Select case b. Select with where clause – a predicate string 
         */
        out.println ();
        out.println("TEST: SELECT CASE B");
        Table select_case_b = movie.select (t -> t[movie.col("title")].equals ("Film_22"));
        select_case_b.print ();
        select_case_b.save ();
        

        /**
         * Select case c Select with key given as attribute
         */
        out.println ();
        out.println("TEST: SELECT CASE C");
        Table select_case_c = movieStar.select (new KeyType ("StarName_25"));
        select_case_c.print ();
        select_case_c.save ();
        
        
        /**
         * Project case a: Project on a key column
         */
        out.println ();
        out.println("TEST: PROJECT CASE A");
        Table project_case_a = movieStar.project ("name");
        project_case_a.print ();
        project_case_a.save ();
        
        /**
         * Project case b: Project on a non-key column: this should eliminate the duplicates 
         * if duplicates are present in the table
         */
        out.println();
        out.println("TEST: PROJECT CASE B");
        Table project_case_b = movie.project ("genre");
        project_case_b.print ();
        project_case_b.save ();
        
      //--------------------- equi-join: movie JOIN studio ON studioName = name
        /**
         * Join case a: Equi join – giving table column names as attributes
         */
        out.println ();
        out.println("TEST: JOIN CASE A");
        Table join_case_a = movie.join ("studioName", "name", studio);
        join_case_a.print ();
        join_case_a.save ();
        

        /**
         *  Join case b - natural join
         */
        out.println ();
        out.println("TEST: JOIN CASE B");
        Table join_case_b = movie.join (cinema);
        join_case_b.print ();
        join_case_b.save ();
        
        
        /**
         * Join case c - Cross product – natural join on two tables without any 
         * common attributes will give a cross product of the two tables
         */
        out.println();
        out.println("TEST: JOIN CASE A");
        out.println("Cross Product: movie and movieStar have no common attributes");
        Table join_case_c = movie.join(movieStar);
        join_case_c.print();
        join_case_c.save();       
      
        /**
         * union case a: simple union operation
         */
        out.println ();
        out.println("TEST: UNION TEST CASE");
        Table union_case_a = movie.union (cinema);
        union_case_a.print ();
        union_case_a.save ();

        //--------------------- minus: movie MINUS cinema
        /**
         * minus case a: simple minus operation
         */
        out.println ();
        out.println("TEST: MINUS TEST CASE");
        Table minus_case_a = movie.minus (cinema);
        minus_case_a.print ();
        minus_case_a.save ();
        
        
//        /******************************* EXTRA TEST CASES *************************/
//        //--------------------- select: !equals
//        /**
//         *@author Ben Rotolo
//         *
//         */
//        out.println ("Testing Select with !equals");
//        out.println ();
//        Table select_case_b_not = movie.select (t -> t[movie.col("title")].equals ("Star_Wars"));
//                                            
//        select_case_b_not.print ();
//        select_case_b_not.save ();
//        
//        //--------------------- select: <
//
//        out.println ();
//        Table select_case_b2 = movie.select (t -> (Integer) t[movie.col("year")] < 1980);
//        select_case_b2.print ();
//
//        
//        //--------------------- select with blank key
//        /**
//         *@author Ben Rotolo
//         *
//         */
//        out.println ("Select with a  ' ' for key...");
//        Table select_case_c_blank = movieStar.select (new KeyType (" "));
//        select_case_c_blank.print ();
//        
//        //--------------------- select without args
//        /**
//         *@author Ben Rotolo
//         *
//         */
//        out.println ("Select with out arguments...");
//        Table select_case_c_no_arg = movieStar.select ();
//        select_case_c_no_arg.print ();

    } // main

} // MovieDB class
