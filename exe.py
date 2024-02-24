from bibliography import db_query

while True:
    
    keyword = input("keyword: ")

    if keyword == "exit":
        break
    else:
        db_query(keyword)
