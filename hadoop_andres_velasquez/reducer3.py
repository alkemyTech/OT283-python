#!/usr/bin/env python3
import sys

#top_users = []

for line in sys.stdin:
    user, fav_count  = line.split('\t')
    user = user.strip()
    fav_count = (fav_count.strip())
    ###print(f'{user}|{fav_count}')


    if len(top_users) >= 10:
        top_users = sorted(top_users, key=lambda x: x[1])
        if fav_count > top_users[0][1]:
            top_users.pop(0)
            top_users.insert(0,[user, fav_count])
    else:
        top_users.append([user, fav_count])

    print(top_users)

top_users = sorted(top_users, key=lambda x: x[1])
with open('top_users_by_favs.txt', 'w') as f:
    for i, x in enumerate(top_users):
        print(f'#{10-i} post_id: {x[0]}, number of answers: {x[1]}')
        f.write(f'#{10-i} post_id: {x[0]}, number of answers: {x[1]}\n')
