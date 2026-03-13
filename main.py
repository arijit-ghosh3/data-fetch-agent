from db.connection import get_connection


def main():
    conn = get_connection()
    print("Agent started. Connected to Azure SQL.")
    conn.close()


if __name__ == "__main__":
    main()
