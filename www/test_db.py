#!/usr/bin/env python
# -*- encoding: utf-8 -*-

__author__ = 'SpringChan'

from models import User, Blog, Comment

from transwarp import db

db.create_engine(user='root', password='123456', database='awesome')

u = User(name='Test', email='test@example.com', password='1234567890', image='about:blank')

u.insert()

print 'new user id:', u.id

u1 = User.find_first('where email=?', 'test@example.com')
print 'find user\'s name:', u1.name

u1.delete()

u2 = User.find_first('where email=?', 'test@example.com')
print 'find user:', u2
