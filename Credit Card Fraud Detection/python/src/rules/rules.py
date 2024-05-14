# List all the functions to check for the rules

# Function to define rules
def rules_check(UCL,score,distance,time_diff,amount):
    if amount < UCL:
        if time_diff < (distance*4):
            if score > 200:
                return True
    return False