DROP TABLE IF EXISTS PostHistory CASCADE;
CREATE TABLE IF NOT EXISTS PostHistory (
    Id                 int  PRIMARY KEY   ,
    PostHistoryTypeId  int                ,
    PostId             int                ,
    RevisionGUID       text               ,
    CreationDate       timestamp not NULL ,
    UserId             int                ,
    UserDisplayName    text               ,
    Comment            text               ,
    Text               Text               ,
    ContentLicense     text               ,
    PostText           text               ,
    jsonfield          JSON
);
