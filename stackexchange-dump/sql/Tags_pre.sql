DROP TABLE IF EXISTS Tags CASCADE;
CREATE TABLE IF NOT EXISTS Tags (
    Id                    int  PRIMARY KEY ,
    TagName               text not NULL    ,
    Count                 int              ,
    ExcerptPostId         int              ,
    WikiPostId            int              ,
    jsonfield             JSON
);
