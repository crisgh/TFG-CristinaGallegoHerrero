-- impossible to enforce these constraints, set as 'not valid' to disable
-- initial test.
--
-- These constaints can be forced running the following queries:
-- ALTER TABLE postlinks ALTER postid DROP NOT NULL;
-- UPDATE postlinks SET postid=NULL WHERE postid NOT IN (SELECT DISTINCT id FROM Posts);
-- ALTER TABLE postlinks VALIDATE CONSTRAINT fk_postlinks_postid;
-- ALTER TABLE postlinks ALTER relatedpostid DROP NOT NULL;
-- UPDATE postlinks SET relatedpostid=NULL WHERE relatedpostid NOT IN (SELECT DISTINCT id FROM Posts);
-- ALTER TABLE postlinks VALIDATE CONSTRAINT fk_postlinks_relatedpostid;
--
SET FOREIGN_KEY_CHECKS = 0;
ALTER TABLE Postlinks ENGINE=InnoDB;
ALTER TABLE Posts ENGINE=InnoDB;
ALTER TABLE Postlinks ADD CONSTRAINT fk_postlinks_postid FOREIGN KEY (postid) REFERENCES posts (id);
ALTER TABLE Postlinks ADD CONSTRAINT fk_postlinks_relatedpostid FOREIGN KEY (relatedpostid) REFERENCES posts (id);
