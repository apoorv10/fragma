# Movie Data analysis
Analyis of Movie Database from MovieLens.org

### Prerequisites
- Apache Spark 1.6
- Scala 2.10

### Data 

MovieLens 1M Dataset
Stable benchmark dataset. 1 million ratings from 6000 users on 4000 movies.
(https://grouplens.org/datasets/movielens/)


### Files
- user.dat (User data)               
- movies.dat (Movies data)           
- ratings.dat (Ratings data)        
- data1.scala
- data2.scala

## Problem Statement

1. Top ten most viewed movies with their movies Name (Ascending or Descending order)
2. Top twenty rated movies (Condition: The movie should be rated/viewed by at least 40 users)
3. We wish to know how have the genres ranked by Average Rating, for each profession and age
group. The age groups to be considered are: 18-35, 36-50 and 50+.

#### data.scala
Solution to Problem 1 and 2:

Top 10 Most Viewed Movies(Top to bottom):
| First Header  | Second Header      |
| ------------- | ------------- |
| Content Cell  | Content Cell  |
| Content Cell  | Content Cell  |

+-------+-----------------------------------------------------+

|MovieID|Title                                                |

+-------+-----------------------------------------------------+

|2858   |American Beauty (1999)                               |

|260    |Star Wars: Episode IV - A New Hope (1977)            |

|1196   |Star Wars: Episode V - The Empire Strikes Back (1980)|

|1210   |Star Wars: Episode VI - Return of the Jedi (1983)    |

|480    |Jurassic Park (1993)                                 |

|2028   |Saving Private Ryan (1998)                           |

|589    |Terminator 2: Judgment Day (1991)                    |

|2571   |Matrix, The (1999)                                   |

|1270   |Back to the Future (1985)                            |

|593    |Silence of the Lambs, The (1991)                     |

+-------+-----------------------------------------------------+

Top 20 Highest Rated Movies(Top to bottom):
+-------+---------------------------------------------------------------------------+------------------+
|MovieID|Title                                                                      |Rating            |
+-------+---------------------------------------------------------------------------+------------------+
|2905   |Sanjuro (1962)                                                             |4.608695652173913 |
|2019   |Seven Samurai (The Magnificent Seven) (Shichinin no samurai) (1954)        |4.560509554140127 |
|318    |Shawshank Redemption, The (1994)                                           |4.554557700942973 |
|858    |Godfather, The (1972)                                                      |4.524966261808367 |
|745    |Close Shave, A (1995)                                                      |4.52054794520548  |
|50     |Usual Suspects, The (1995)                                                 |4.517106001121705 |
|527    |Schindler's List (1993)                                                    |4.510416666666667 |
|1148   |Wrong Trousers, The (1993)                                                 |4.507936507936508 |
|922    |Sunset Blvd. (a.k.a. Sunset Boulevard) (1950)                              |4.491489361702127 |
|1198   |Raiders of the Lost Ark (1981)                                             |4.477724741447892 |
|904    |Rear Window (1954)                                                         |4.476190476190476 |
|1178   |Paths of Glory (1957)                                                      |4.473913043478261 |
|260    |Star Wars: Episode IV - A New Hope (1977)                                  |4.453694416583082 |
|1212   |Third Man, The (1949)                                                      |4.452083333333333 |
|750    |Dr. Strangelove or: How I Learned to Stop Worrying and Love the Bomb (1963)|4.4498902706656915|
|720    |Wallace & Gromit: The Best of Aardman Animation (1996)                     |4.426940639269406 |
|1207   |To Kill a Mockingbird (1962)                                               |4.425646551724138 |
|3435   |Double Indemnity (1944)                                                    |4.415607985480944 |
|912    |Casablanca (1942)                                                          |4.412822049131217 |
|670    |World of Apu, The (Apur Sansar) (1959)                                     |4.410714285714286 |
+-------+---------------------------------------------------------------------------+------------------+

#### data2.scala
Solution to Problem 3:

Genre Ranking by Average Rating:
+--------------------+-----+-----------+-----------+-----------+-----------+-----------+
|Occupation          |Age  |Rank1      |Rank2      |Rank3      |Rank4      |Rank5      |
+--------------------+-----+-----------+-----------+-----------+-----------+-----------+
|farmer              |18-35|Western    |War        |Romance    |Mystery    |Film-Noir  |
|farmer              |50+  |Western    |Film-Noir  |War        |Romance    |Drama      |
|writer              |18-35|Film-Noir  |Documentary|War        |Animation  |Crime      |
|writer              |50+  |Documentary|Film-Noir  |War        |Drama      |Crime      |
|K-12 student        |36-50|Documentary|Romance    |War        |Comedy     |Drama      |
|programmer          |36-50|Film-Noir  |War        |Animation  |Documentary|Drama      |
|unemployed          |36-50|Film-Noir  |War        |Mystery    |Crime      |Comedy     |
|academic/educator   |18-35|Documentary|Film-Noir  |War        |Animation  |Drama      |
|academic/educator   |50+  |Film-Noir  |Documentary|War        |Mystery    |Drama      |
|executive/managerial|18-35|Film-Noir  |Documentary|War        |Drama      |Crime      |
|executive/managerial|50+  |Film-Noir  |War        |Documentary|Mystery    |Drama      |
|customer service    |36-50|Film-Noir  |Animation  |War        |Drama      |Musical    |
|artist              |36-50|Film-Noir  |Documentary|War        |Mystery    |Animation  |
|retired             |36-50|War        |Film-Noir  |Western    |Drama      |Thriller   |
|doctor/health care  |18-35|War        |Film-Noir  |Crime      |Drama      |Mystery    |
|doctor/health care  |50+  |Film-Noir  |War        |Documentary|Drama      |Western    |
|other               |36-50|Film-Noir  |Documentary|War        |Animation  |Drama      |
|college/grad student|18-35|Film-Noir  |Documentary|War        |Drama      |Crime      |
|college/grad student|50+  |Documentary|Musical    |Western    |Film-Noir  |Children's |
|lawyer              |18-35|Documentary|Film-Noir  |War        |Drama      |Western    |
|lawyer              |50+  |Documentary|Film-Noir  |War        |Musical    |Mystery    |
|clerical/admin      |36-50|Film-Noir  |Musical    |War        |Animation  |Drama      |
|sales/marketing     |18-35|Film-Noir  |Documentary|War        |Animation  |Drama      |
|sales/marketing     |50+  |Film-Noir  |Drama      |Crime      |War        |Musical    |
|technician/engineer |18-35|Film-Noir  |Documentary|War        |Drama      |Crime      |
|technician/engineer |50+  |Film-Noir  |Animation  |Documentary|War        |Musical    |
|scientist           |36-50|Film-Noir  |Documentary|War        |Drama      |Western    |
|homemaker           |18-35|War        |Drama      |Romance    |Documentary|Mystery    |
|homemaker           |50+  |Animation  |Children's |Musical    |War        |Mystery    |
|K-12 student        |18-35|Film-Noir  |Animation  |War        |Musical    |Drama      |
|K-12 student        |50+  |Documentary|Film-Noir  |Mystery    |Drama      |Musical    |
|programmer          |18-35|Film-Noir  |War        |Documentary|Drama      |Western    |
|programmer          |50+  |Film-Noir  |Mystery    |War        |Drama      |Thriller   |
|unemployed          |18-35|Film-Noir  |Documentary|War        |Crime      |Drama      |
|unemployed          |50+  |Film-Noir  |Animation  |Mystery    |Western    |Crime      |
|self-employed       |36-50|Film-Noir  |Documentary|War        |Drama      |Crime      |
|customer service    |18-35|Film-Noir  |Documentary|War        |Animation  |Drama      |
|customer service    |50+  |Film-Noir  |Crime      |Mystery    |Drama      |Western    |
|artist              |18-35|Film-Noir  |Documentary|War        |Drama      |Mystery    |
|artist              |50+  |War        |Musical    |Crime      |Film-Noir  |Drama      |
|retired             |50+  |Film-Noir  |War        |Documentary|Mystery    |Drama      |
|other               |18-35|Film-Noir  |Documentary|War        |Drama      |Animation  |
|other               |50+  |Film-Noir  |War        |Drama      |Musical    |Western    |
|tradesman/craftsman |36-50|Western    |War        |Film-Noir  |Animation  |Documentary|
|farmer              |36-50|Film-Noir  |Documentary|Musical    |War        |Animation  |
|writer              |36-50|Film-Noir  |Documentary|Musical    |War        |Animation  |
|academic/educator   |36-50|Film-Noir  |War        |Documentary|Drama      |Mystery    |
|executive/managerial|36-50|Film-Noir  |War        |Documentary|Drama      |Crime      |
|clerical/admin      |18-35|Film-Noir  |War        |Documentary|Musical    |Animation  |
|clerical/admin      |50+  |Film-Noir  |War        |Documentary|Animation  |Mystery    |
|doctor/health care  |36-50|Documentary|War        |Film-Noir  |Drama      |Crime      |
|college/grad student|36-50|Western    |War        |Documentary|Film-Noir  |Musical    |
|scientist           |18-35|Film-Noir  |Documentary|War        |Animation  |Western    |
|scientist           |50+  |Film-Noir  |Drama      |Crime      |War        |Sci-Fi     |
|lawyer              |36-50|Film-Noir  |Documentary|Mystery    |War        |Crime      |
|sales/marketing     |36-50|Film-Noir  |Documentary|War        |Western    |Drama      |
|self-employed       |18-35|Film-Noir  |War        |Documentary|Animation  |Drama      |
|self-employed       |50+  |Film-Noir  |Documentary|War        |Drama      |Animation  |
|technician/engineer |36-50|Documentary|Film-Noir  |War        |Drama      |Animation  |
|homemaker           |36-50|Documentary|War        |Musical    |Drama      |Romance    |
|tradesman/craftsman |18-35|Film-Noir  |War        |Drama      |Animation  |Western    |
|tradesman/craftsman |50+  |War        |Film-Noir  |Adventure  |Musical    |Sci-Fi     |
+--------------------+-----+-----------+-----------+-----------+-----------+-----------+








Authors
Billie Thompson - Initial work - PurpleBooth
See also the list of contributors who participated in this project.

License
This project is licensed under the MIT License - see the LICENSE.md file for details

Acknowledgments
Hat tip to anyone who's code was used
Inspiration
etc
