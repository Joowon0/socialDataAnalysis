# Dynamic social data analysis using event processing systems


## Getting Started

The purpose of this project is to find three most living posts in social network. Also, it should be able to find the biggest community that is discussing hot topic. If data is given, the new result should be updated in real-time.

### Prerequisites

More than one computer is needed to excute this program. To proceed in distributed way, two computers is neccessary: one master computer, one or more than one slave computer. All of the computers need Apache Spark to be installed.

Step1. If the you want to execute the program in server computer, you better make the file into a jar file. Making it into a jar file requires sbt. Try the following command after installing scala on the computer. (Check how in Installing session)
```
sudo apt-get install sbt
```

Step2-1. Before making a jar file, a slight change is required. There are two function call instructions in 'socialDataAnalysis/src/main/scala/wikipedia/WikipediaRanking.scala'. If you want to execute Query1, make Query2 funciton call into a comment. If you want to execute Query2, make Query1 function call into a comment. If neither of them is commented, it will make an error.

Step2-2. If you are trying in your own computer with no slave computers, this part can be skipped. If you are running the program with slave computers, you should set IP address in the code. In Both file 'socialDataAnalysis/src/main/scala/wikipedia/Query1.scala' and 'socialDataAnalysis/src/main/scala/wikipedia/Query2.scala', comment out line 16 and comment in line 15. And then change the IP address in line 15 into master computer's.

Step2-3. The data files need to be stored in all of the computers, including master computer and slave computers. The location of the data files is '/home/ana/data/' in each computer. You can check the exact location in line 57 ~ 58 in 'socialDataAnalysis/src/main/scala/wikipedia/Query1.scala' and in line 58 ~ 60 in 'socialDataAnalysis/src/main/scala/wikipedia/Query2.scala'.


Step3. Try the following in the project file to make a jar file.
```
sbt package
```

### Installing

Step1. There should be jdk installed. The program works on 1.7 or higher version. But jdk 1.8 is highly recommended.
```
sudo apt-get install defualt-jdk
```

Step2. All computeres need Scala to be installed.
```
sudo apt-get install scala
```

Step3. Apache Spark is required for execution.
```
sudo apt-get install spark
```

## Running the tests

Try the following command in Spark directory.
```
./bin/spark-submit PATH-TO-JAR-FILE
```

### Query1
The goal of query 1 is to compute the top-3 scoring active posts, producing an updated result every time they change.
The total score of an active post P is computed as the sum of its own score plus the score of all its related comments. A comment C is related to a post P if it is a direct reply to P or if the chain of C's preceding messages links back to P.
Each new post has an initial own score of 10 which decreases by 1 each time another 24 hours elapse since the post's creation. Each new comment's score is also initially set to 10 and decreases by 1 in the same way (every 24 hours since the comment's creation). Both post and comment scores are non-negative numbers, that is, they cannot drop below zero. A post is considered no longer active (that is, no longer part of the present and future analysis) as soon as its total score reaches zero, even if it receives additional comments in the future.

### Query2
This query addresses the change of interests with large communities.
Given an integer k and a duration d (in seconds), find the k comments with the largest range, where the range of a comment is defined as the size of the largest connected component in the graph defined by persons who (a) have liked that comment (see likes, comments), (b) where the comment was created not more than d seconds ago, and (c) know each other (see friendships).
k in 5 and d is 7 days in this program.

### Coding Style

The coding style comply http://www.scalastyle.org/


## Built With

* [Scala](https://www.scala-lang.org/) - Functional Programming Language
* [Spark](https://spark.apache.org/) - Distributed System Framework


## Authors

* **Gayoung Gim** - *Visualization*
* **Joowon Byun** - *Programming in Scala*
* **Hyeong Rok** - *Environment Setting*
