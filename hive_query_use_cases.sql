
#########################################################################################
#
#   Query 1: At-Risk Customers
#
#   Identify all customers 
#     where state = "NC" 
#     and 
#     average spend >= $2000 
#     and 
#     they've contacted the call center at least 2 times in the last month
#
#########################################################################################

set tez.runtime.io.sort.mb = 2000;

SELECT customer_agg.*, at_risk_customers.monthly_spend, at_risk_customers.month_calls_to_agent
FROM
    (select 
        customer_id, substr(`date`,0,7) as mm_yy, sum(called_agent) as month_calls_to_agent, sum(transaction) as monthly_spend
        from customer_agg
        where state == 'NC'
        group by customer_id, substr(`date`,0,7)
        having month_calls_to_agent >= 2 and monthly_spend > 2000
        order by monthly_spend desc, month_calls_to_agent desc
    ) as at_risk_customers
LEFT JOIN customer_agg 
ON customer_agg.customer_id=at_risk_customers.customer_id
WHERE `date` >= '2017-09-01' and `date` < '2017-10-01'
LIMIT 50;



#########################################################################################
#
#   Query 2: Younger Generation / High Lifetime Value
#
#   Give me all customers (based on customer_id) 
#       where age is 20-29 
#       and
#       average transaction amount >= $2000 per month
#       and
#       total number of transactions within the past 3 years >= 10
#
#########################################################################################

set tez.runtime.io.sort.mb = 2000;

SELECT customer_agg.*, high_lifetime_value.total_transactions, high_lifetime_value.avg_monthly_spend
FROM
    (select 
        customer_id, age, count(*) as total_transactions, sum(transaction)/36 as avg_monthly_spend
        from customer_agg
        where age >= 20 and age < 30
        group by customer_id, age
        having total_transactions >= 10 and sum(transaction)/36 > 2000
        order by avg_monthly_spend desc, total_transactions desc
    ) as high_lifetime_value
LEFT JOIN customer_agg 
ON customer_agg.customer_id=high_lifetime_value.customer_id
LIMIT 50;


#ZEND
