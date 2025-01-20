# LeetCode Contest Analytics
The following observations are conducted with **Amazon Redshift**.

## Data Overview
- The collected data are from the top **50,000** users of LeetCode global contest ranking and **2334** problems 
of **585** contests.
- The range of ratings in the data collected is **1788 - 3700**.

## Duplicated Users
- There are roughly **60** users that appear more than once in the collected data, not including _NULL_ or deleted users.
- Their accounts exist in both US and CN data regions.

## Countries by Number of Users
![](./images/user_distribution.jpeg)
- Approximately **15,600** users are from unknown countries.
- China and India are the two largest populations in the world. The markets in these countries are probably more 
competitive as well.
- These top countries are likely more competitive and developed in the tech market.

## Average Rating of Top Countries by Number of Users
![](images/avg_rating.jpeg)
- There's no correlation between the number of users and the average rating of a country among the top 50,000 users.
- LeetCode might not be popular in some countries, where they prefer other competitive programming platforms.

## Average Ranking by Rating Bracket
![](./images/avg_bracket_ranking.jpeg)
- _Average ranking_ is the average placement of all contests a user attended.
- Overall, the average ranking distribution is pretty diverse, except for the top contestants.
- The lower the rating, the more diverse the average ranking.
- It can be inferred from the data that users with higher ratings usually perform more consistently across contests.
- Contestants can predict their future growth and potential rating bracket based on this graph. Eg: A person with a
current rating of 2000 and an average ranking of 100 can possibly reach 3250+ without difficulty.

## Average Number of Contests Attended by Rating Bracket
![](images/avg_contest_count.jpeg)
- It can be inferred from the graph that it takes more contests to reach a higher rating.
- Among these rating brackets, _3250 - 3499_ seems to be the hardest to reach.
- _3500+_ seems to take fewer contests to reach, likely because people in this rating bracket are more experienced 
competitive programmers who practiced and attended a lot of contests on other platforms and in real life.

## Topics by Number of Appearances
![](./images/topics.jpeg)
- These topic tags show up most frequently during contests.
- The topic tags in LeetCode might be insufficient. Eg: A problem uses the line sweep technique, but it's not tagged line sweep.
- Practicing these topics accordingly might improve your average contest placement.