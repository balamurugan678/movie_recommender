# MOVIE RECOMMENDER SERVICE - ITEM-BASED COLLABORATIVE FILTERING RECOMMENDATION

* The movie recommender service as the name implies recommends Movie based on the Item and User Ratings.
* The Dataset we are analysing is the movielens dataset.
* The similarities have been found using Cosine Similarity algorithm
* The Score threshold and Cooccurence threshold can be configured to get the top results


## Where are the datasets

* The datasets are from Movielens - https://grouplens.org/datasets/movielens/ and are stored in the datasets directory.
* The movies.csv/.dat are the movies data and ratings.csv/.dat are the ratings data.

## Rating File description

All ratings are contained in the file "ratings.csv" and are in the following format:

UserID::MovieID::Rating::Timestamp


## Movies File description

Movie information is in the file "movies.csv" and is in the following format:

MovieID::Title::Genres


## How to build the application

* The application needs to be built using **MAVEN** and the below command should be run in any machine which has maven installed in it. Go to the directory of the code and execute the below maven command

```
cd /****CODE_DIRECTORY****/

mvn clean install
```

* Please refer -https://maven.apache.org/install.html if you haven't had maven installed in the machine


## How to run the application

* Once the previous step has been completed, we can run the application using the below command
* The three elements which are followed by the JAR name(movie-recommender.jar) are program arguments. The first element the movie id which needs the output recommendation, second one is scorethreshold and the third one is cooccurrence threshold.

```
cd /****CODE_DIRECTORY/target****/

java -jar 100 0.5 5.0 movie-recommender.jar
```
