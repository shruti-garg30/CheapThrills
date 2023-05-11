# Big Data Project Submission
### CheapThrills
#### Members 
- Meenesh Solanki (mms9905)
- Purvav Punyani (psp8474)
- Sheena Garg (sg7394)
- Shruti Garg (sg7395)
- Tiyas Dey (td2355)


#### Introduction

Welcome to Cheap Thrills. This README file provides detailed instructions on how to clone the repository, set up the project, and run the application on your local machine.

#### Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

#### Prerequisites
Ensure that you have the following installed on your local machine:

- Git 
  - https://github.com/git-guides/install-git
- Python and pip
- Flask
- Node.js and npm
  - https://nodejs.org/en/download
- Docker
  - Docker: https://www.docker.com/
  - Windows 7, 10 Home: Docker Toolbox: https://www.docker.com/products/docker-toolbox
  - Windows 10 Pro: Docker for Windows: https://docs.docker.com/docker-for-windows/
  - Mac OS: Docker for Mac: https://www.docker.com/products/docker#/mac
  - Linux: containers (Linux kernel 3.6+): https://docs.docker.com/engine/installation/linux/
  - Windows required virtualization enabled in BIOS
  - Windows 10 Pro: required HyperV virtualization installed/enabled)

Installation
Follow these steps to get a development environment running:

1 - Clone the repository
To clone the repository, run the following command in your terminal:
- git clone https://github.com/shruti-garg30/CheapThrills

2 - Install the dependencies
You need to run a script file named 'setup.sh' to install all the necessary dependencies. Here's how you can do this:
For Windows:
Open your command prompt and navigate to the directory of the cloned repo, then run:
- .\setup.sh

For Mac:
Open your terminal and navigate to the directory of the cloned repo, then run:
- bash setup.sh

3 - Start the Flask server
Navigate to 'flask-server' directory in your terminal, then start the server by running:
- flask run -h localhost -p 8000
This command will start the Flask server at localhost on port 8000.

4 - Start the client
In a different terminal window, navigate to the 'client' folder, then start the client by running:
- npm start

We hope that these instructions help you to get the project up and running on your local machine. If you encounter any issues, please feel free to open an issue in this repository.

Happy coding!


