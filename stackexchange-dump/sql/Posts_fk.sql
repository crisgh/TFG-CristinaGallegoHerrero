SET FOREIGN_KEY_CHECKS = 0;
ALTER TABLE Users ENGINE=InnoDB;
ALTER TABLE Posts ENGINE=InnoDB;
ALTER TABLE Posts ADD CONSTRAINT fk_posts_parentid FOREIGN KEY (parentid) REFERENCES posts (id);
ALTER TABLE Posts ADD CONSTRAINT fk_posts_owneruserid FOREIGN KEY (owneruserid) REFERENCES users (id);
ALTER TABLE Posts ADD CONSTRAINT fk_posts_lasteditoruserid FOREIGN KEY (lasteditoruserid) REFERENCES users (id);
