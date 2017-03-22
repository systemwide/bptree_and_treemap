*Team Potato Project 3*
- Zach Saucier
- Layton Hayes
- Jeff Cardinal
- Benjamin Rotolo

There are two directories for our submission:
BPTree and
Treemap

MovieDB.java is the entry point for both implementations, and output should be as
readable .txt files thanks to Divya's code snippets on Piazza.

*Test Cases and Corresponding .txt Files*

Select: 
* CASE A. Select without any where clause, i.e., no attributes specified in the method (movie.txt)
* CASE B. Select with where clause – a predicate string (movie0.txt)
* CASE C. Select with key given as attribute (movieStar1.txt)

Project:
* CASE A. Project on a key column (movieStar2.txt)
* CASE B. Project on a non-key column: this should eliminate the duplicates if duplicates are present in the table (movie3.txt)

Join: 
* CASE A. Equi join – giving table column names as attributes (movie4.txt)
* CASE B. Natural join – without any column names given; the join should identify which columns to pick for comparison 
(movie5.txt)
* CASE C. Cross product – natural join on two tables without any common attributes will give a cross product of the two tables
(movie6.txt)

* Union: no specific test cases other than the compatibility check and elimination of duplicates (movie7.txt)

* Minus: simple minus operation with no specific test cases (movie8.txt)
