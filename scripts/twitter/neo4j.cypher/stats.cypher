profile match (n:twitterUser) where n.screen_name is null
return count (n) as to_import
