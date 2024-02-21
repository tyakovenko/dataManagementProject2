import random
import pandas as pd

#k file
k = random.randint(0,100)
#kPoints = [random.randint(0, 10000) for _ in range(k)]

# Generate an array of random values between 0 and 5000
x = [random.randint(0, 5000) for _ in range(3200)]
y = [random.randint(0, 5000) for _ in range(3200)]

xk = [random.randint(0, 10000) for _ in range(k)]
yk = [random.randint(0, 10000) for _ in range(k)]

#write onto a data frame
# Create a DataFrame
xy = {'x': x, 'y': y}
xyk = {'x': xk, 'y': yk}
df = pd.DataFrame(xy)
dfk = pd.DataFrame(xyk)
#kDf = pd.DataFrame({'k': kPoints})


#write onto a text file
# Write the DataFrame to a text file
df.to_csv('xy.txt', sep='\t', index=False)
dfk.to_csv('xyk.txt', sep='\t', index=False)
#kDf.to_csv('k.txt', sep='\t', index=False)