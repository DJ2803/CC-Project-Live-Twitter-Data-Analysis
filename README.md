# CC-Project-Live-Twitter-Data-Analysis

The motivation behind the "Live Twitter Data Analysis using Cloud Services" project is to harness the power of cloud computing to extract, process, and analyze real-time data from Twitter. Twitter is one of the most popular social media platforms with millions of users tweeting every day. By leveraging cloud services, the project aims to provide a scalable, efficient, and cost-effective solution to analyze tweets and gain insights into trending topics, sentiment analysis, and social media behavior.

 

The purpose of this project is to create a pipeline that can extract tweets from Twitter, store them in an S3 bucket, perform data transformations and pre-processing using pandas in Lambda, generate visualizations using plotly, and store the results back in S3. This pipeline will be triggered using CloudWatch, which will monitor for incoming tweets and trigger the processing pipeline when new tweets are detected. An interactive dashboard will be hosted using Elastic Beanstalk (EBS), which will allow users to explore the data and gain insights. EC2 will be used to install additional libraries and dependencies that may be required for processing and analysis.
