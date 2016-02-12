
# 1) Receive HTTP request
# 2) If cached, reply with cache
# 3) If continuation ,
#   if) Lookup OAuth info from previous request
#   else) pop oauth info from redis endpoint Queue
# 4) hash authentication header
# 5) Wait until user-endpoint timout expires
# 6) deliver request
# 7) receive response
# 8) If response...
#   error, twitter down) send keepalive, retry
#   error, slow down) send keepalive, wait, retry
#   unauthorized) log to handle private users in the future
#   success) store in cache, relay response

# https://github.com/carlosabalde/libvmod-redis

# attempt graceful response if backend is down
sub vcl_recv {
  if (req.backend.healthy) {
    set req.grace = 30s;
  } else {
    set req.grace = 1h;
  }
}

sub vcl_fetch {
   set beresp.grace = 1h;
}

# code taken from https://adayinthelifeof.nl/2012/07/06/using-varnish-to-offload-and-cache-your-oauth-requests/
# originally meant to authenticate clients before sending to internal app
# change to set authentication header on request before being sent upstream
sub vcl_hash {
  if (req.http.x-auth-token && req.backend == oauth) {
      hash_data("TOKEN " + req.http.x-auth-token);
      return(hash);
  }
  if (req.http.x-api-user) {
    hash_data(req.http.x-api-user);
    hash_data(req.http.x-api-context);
  }
}

sub vcl_miss {
    if (req.http.x-auth-token && req.backend == oauth) {
        set bereq.url = "/checktoken.php";
        set bereq.request = "HEAD";
    }
}

sub vcl_fetch {
    if (req.http.x-auth-token && req.backend == oauth) {
        if (beresp.status != 200) {
            error 401 "Not Authorized";
        }
        set req.http.x-api-user = beresp.http.x-api-user;
      }
    set req.http.x-api-context = beresp.http.x-api-context;

    set req.http.x-restart = "1";

    return(deliver);
}

sub vcl_deliver {
    if (req.http.x-restart) {
        unset req.http.x-restart;
        return(restart);
    }
}

sub vcl_hit {
    if (req.http.x-auth-token && req.backend == oauth) {
        set req.http.x-api-user = obj.http.x-api-user;
        set req.http.x-api-context = obj.http.x-api-context;

        set req.http.x-restart = "1";
    }
}

# twitter backends
backend b_API_HOST {
        .host = "https://api.twitter.com";
        .probe = {
                .url = "/";
                .timeout = 34 ms;
                .interval = 1s;
                .window = 10;
                .threshold = 8;
        }
}
backend b_REST_ROOT {
        .host = "https://api.twitter.com";
        .probe = {
                .url = "/1.1/";
                .timeout = 34 ms;
                .interval = 1s;
                .window = 10;
                .threshold = 8;
        }
}
backend b_PUB_STREAM {
        .host = "https://stream.twitter.com";
        .probe = {
                .url = "/1.1/";
                .timeout = 34 ms;
                .interval = 1s;
                .window = 10;
                .threshold = 8;
        }
}
backend b_USER_STREAM {
        .host = "https://userstream.twitter.com";
        .probe = {
                .url = "/1.1/";
                .timeout = 34 ms;
                .interval = 1s;
                .window = 10;
                .threshold = 8;
        }
}
backend b_SITE_STREAM {
        .host = "https://sitestream.twitter.com";
        .probe = {
                .url = "/1.1/";
                .timeout = 34 ms;
                .interval = 1s;
                .window = 10;
                .threshold = 8;
        }
}
backend b_MEDIA_UPLOAD {
        .host = "https://upload.twitter.com";
        .probe = {
                .url = "/1.1/";
                .timeout = 34 ms;
                .interval = 1s;
                .window = 10;
                .threshold = 8;
        }
}
backend b_OA_REQ {
        .host = "https://api.twitter.com";
        .probe = {
                .url = "/oauth/request_token";
                .timeout = 34 ms;
                .interval = 1s;
                .window = 10;
                .threshold = 8;
        }
}
backend b_OA_ACCESS {
        .host = "https://api.twitter.com";
        .probe = {
                .url = "/oauth/access_token";
                .timeout = 34 ms;
                .interval = 1s;
                .window = 10;
                .threshold = 8;
        }
}
