## Work Sample for Data Role, Apache-Spark Variant
**About myself and how I believe I can grow from this internship**

I am a second year Mathematics & Statistics student currently studying at University of Toronto. I wish to use the opportunity of internship or workspace environment to further extend my knowledge on the field of data science and continuing my career as a data scientist.

The given sample problems made it clear to me that EQ Works is like a treasure box that contains countless amount of invaluable knowledge that I yet to discovered and learned. Prior working on the problems, I have a limited knowledge on data analyzing from the statical and mathematical courses I had taken in my previous years. As a result, I found each problem as a huge wall to climb over; In order to overcome these obstacles I am forced to be creative by utilizing the knowledge I gained from the classrooms and further extending them to create solutions that I would never learned from working through the textbook questions. 

Although I was introduced to the data science in one of the statistical courses I had taken before, I never understood the importance of algorithm runtime in data analyzing. Especially when I was solving certain problems such as problem 2 and 4b, I realized that while there are brute force methods of solving the problems, but the completion time would be unacceptable. For instance, I could assign the closest POI to each of the requests by using 2 for loops to iterate through all of the possibilities. However, after reading over the documentation on Apache Spark and google searching, I realized that the problem could be solved in a an efficient way by taking advantage of Apache Spark's computing performance to compute, wrangle and filter data. By taking advantage of Apache Spark's technology I was able to assign each request to its closest POI without using any for loop. 

While there are some hair pulling moments on my journey to search for an efficient solution for the problems, I found myself enjoying the ride. It would be a pleasure to work on similar problems in the near future. I hope that we may be able to discuss this opportunity soon.

Yung Sheng Cheng 

## About
**Required modules**

```text
pip install -r requirements.txt
```

**Solution Structure**

Note that the solutions for its corresponding problems are constructed with modularity in mind, therefore each solution is packaged in separate modules. In addition each solution checks for the completion of required prerequisites before executing its own tasks. This means each solutions / modules can be ran independently.

Refer to the following examples to execute each solution: 

1. Cleanup

    ```text
    spark-submit /tmp/data/cleanup.py 
    ```
2. Label
    
    ```text
    spark-submit /tmp/data/label.py
    ```
3. Analysis

    ```text
    spark-submit /tmp/data/analysis.py
   ```
4. Data Science/Engineering Tracks
    
    4a. Model
        
        spark-submit /tmp/data/model.py
        
    4b. Pipeline Dependency
    
        spark-submit --packages graphframes:graphframes:0.7.0-spark2.4-s_2.11 /tmp/data/pipeline_dependency.py
