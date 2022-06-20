DROP TABLE IF EXISTS Badges;
CREATE TABLE IF NOT EXISTS Badges (
   Id                int         PRIMARY KEY ,
   UserId            int         not NULL    ,
   Name              text        not NULL    ,
   Date              timestamp   not NULL    ,
   Class             int                     ,
   TagBased          VARCHAR(255)                 ,
   jsonfield         JSON
);
