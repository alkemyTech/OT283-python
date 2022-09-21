Create an .env file with this description:
    USER=your_user_to_postgresql_db
    PASSWORD=your_password_to_postgresql_db
    HOST=your_host
    PORT=your_port
    DB_NAME=your_database_name

    NOTE:
    Do not use space after = symbol


If .env file doesn't work:
    Go to Line 46 in OT283-48.py file. You should see this: 
        engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")
        With user,password, host, port and db_name as variable.

        You should change it manually so it should looks like this:
        engine = create_engine("postgresql://your_user_to_postgresql_db:your_password_to_postgresql_db@your_host:your_port/your_database_name")
    
    Repeat this process at Line 66 of OT283-48.py file

If the problem persist contact to: contact@mail.com




