select s1.* 
from allquotes s1

inner join 
(
  SELECT stock, max(timestamp) as mts
  FROM allquotes
	
  GROUP BY stock 
) s2 on s2.stock = s1.stock and s1.timestamp = s2.mts