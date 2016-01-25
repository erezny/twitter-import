
FROM node:5.2-slim

MAINTAINER Elliott Rezny <erezny@gmail.com>

ENV user node
RUN groupadd --system $user && useradd --system --create-home --gid $user $user

# Run npm install before building docker image
RUN mkdir /home/$user/app /home/$user/server
ADD ./ /home/$user/app
RUN chown -R $user:$user /home/$user/*

USER $user

# Define mountable directories.
VOLUME ["/home/$user/server"]

# Define working directory.
WORKDIR /home/$user/server

# Expose ports.
EXPOSE 80

ENTRYPOINT ["node", "/home/node/app/app.js"]
CMD ["--prod"]
