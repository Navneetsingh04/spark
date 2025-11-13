def signup_user():
    while True:
        email = input("Enter your email for signup (or type 'exit' to quit): ")
        if email.lower() == 'exit':
            print("Existing signup process!")
            break

        with open("email.txt","a") as file:
            file.write(email + "\n")

        print(f"Email {email} stored sucessfully")


if __name__ == "__main__":
    signup_user()
