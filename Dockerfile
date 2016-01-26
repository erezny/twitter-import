
FROM node:5.2-slim

MAINTAINER Elliott Rezny <erezny@gmail.com>

ENV user node
RUN groupadd --system $user && useradd --system --create-home --gid $user $user

# Define mountable directories.
VOLUME ["/home/$user/server"]

# Define working directory.
WORKDIR /home/$user/server

# Expose ports.
EXPOSE 80

# Run npm install before building docker image
RUN mkdir /home/$user/app
ADD ./ /home/$user/app
RUN chown -R $user:$user /home/$user/*

USER $user

ENTRYPOINT ["node", "/home/node/app/app.js"]
