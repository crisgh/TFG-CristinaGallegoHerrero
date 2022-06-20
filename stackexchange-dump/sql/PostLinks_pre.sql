DROP TABLE IF EXISTS PostLinks CASCADE;
CREATE TABLE IF NOT EXISTS PostLinks (
   Id                int         PRIMARY KEY ,
   CreationDate      timestamp   not NUll    ,
   PostId            int         not NULL    ,
   RelatedPostId     int         not NULL    ,
   LinkTypeId        int         not Null    ,
   jsonfield         JSON
);
