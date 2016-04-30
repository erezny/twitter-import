
FROM node:4.2-slim

MAINTAINER Elliott Rezny <erezny@gmail.com>

# Define working directory.
WORKDIR /data/app

# Expose ports.
EXPOSE 80

# Run npm install before building docker image
ADD ./ /data/app

ENTRYPOINT ["npm", "run"]
CMD "start"
