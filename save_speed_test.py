
import pickle
import cPickle as c_pickle
import h5py
import shelve
import sqlite3
import pandas

import os
import time
import matplotlib.pyplot as plt


import numpy as np

def save_by_pickle(data, mat_size):
    f = open("save/pickle/pickle" + str(mat_size) + ".pkl", 'wb')
    pickle.dump(data, f)
    f.close()

def load_by_pickle(mat_size):
    f = open("save/pickle/pickle" + str(mat_size) + ".pkl", 'rb')
    data = pickle.load(f)
    f.close()
    return data

def save_by_cpickle(data, mat_size):
    f = open("save/cpickle/cpickle" + str(mat_size) + ".pkl", 'wb')
    c_pickle.dump(data, f)
    f.close()

def load_by_cpickle(mat_size):
    f = open("save/cpickle/cpickle" + str(mat_size) + ".pkl", 'rb')
    data = c_pickle.load(f)
    f.close()
    return data


def save_by_npy(data, mat_size):
    np.save("save/numpy/numpy" + str(mat_size) + ".npy", data)

def load_by_npy(mat_size):
    data = np.load("save/numpy/numpy" + str(mat_size) + ".npy")
    return data

def save_by_hdf5(data, mat_size):
    f = h5py.File("save/h5py/h5py" + str(mat_size) + ".h5",'w')
    f.create_dataset("data",data=data)
    f.flush()
    f.close()

def load_by_hdf5(mat_size):
    f = h5py.File("save/h5py/h5py" + str(mat_size) + ".h5", "r")
    data = f["data"].value
    f.flush()
    f.close()
    return data

def save_by_shelve(data, mat_size):
    f = shelve.open("save/shelve/shelve" + str(mat_size))
    f["data"] = data
    f.close()

def load_by_shelve(mat_size):
    f = shelve.open("save/shelve/shelve" + str(mat_size))
    data = f["data"]
    f.close()
    return data

def prepare_sqlite():
    con = sqlite3.connect("save/sqlite/sqlite.db")
    try:
        sql = "DROP TABLE data_tb;"
        con.execute(sql)
    except:
        pass
    sql = "CREATE TABLE data_tb (mat_size INT PRIMARY KEY, data BLOB);"
    con.execute(sql)
    con.close()


def save_by_sqlite(data, mat_size):
    con = sqlite3.connect("save/sqlite/sqlite.db")
    sql = 'INSERT INTO data_tb VALUES(?, ?)'
    blob = buffer(data)
    con.execute(sql, (mat_size, blob))
    con.commit()
    con.close()

def load_by_sqlite(mat_size):
    con = sqlite3.connect("save/sqlite/sqlite.db")
    cur = con.cursor()
    sql = 'SELECT data FROM data_tb WHERE mat_size = ?'
    cur.execute(sql, (mat_size,))
    data = cur.fetchone()[0]
    return np.frombuffer(data).reshape(mat_size, mat_size)

def save_speed_test(my_func, mat_size):
    print str(my_func)
    print mat_size
    data = np.random.rand(mat_size, mat_size)
    start = time.time()
    my_func(data, mat_size)
    end = time.time()
    interval = end - start
    print "finished in %.5f sec" % interval
    return interval

def load_speed_test(my_func, mat_size):
    print str(my_func)
    print mat_size
    start = time.time()
    data = my_func(mat_size)
    end = time.time()
    interval = end - start
    print data.shape
    print "finished in %.5f sec" % interval
    return interval


min_mat_size = 100
max_mat_size = 4000
step_mat_size = 100

# create directories
modules = ["pickle","h5py","numpy", "cpickle", "shelve", "sqlite"]
for dir_name in modules:
    if not os.path.isdir("save/" + dir_name):
        print "creating directory: %s" % "save/" + dir_name
        os.mkdir("save/" + dir_name)

prepare_sqlite()

# save & load
save_pickle_times = [save_speed_test(save_by_pickle, i) for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)]
load_pickle_times = [load_speed_test(load_by_pickle, i) for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)]
save_npy_times = [save_speed_test(save_by_npy, i) for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)]
load_npy_times = [load_speed_test(load_by_npy, i) for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)]
save_h5py_times = [save_speed_test(save_by_hdf5, i) for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)]
load_h5py_times = [load_speed_test(load_by_hdf5, i) for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)]
save_shelve_times = [save_speed_test(save_by_shelve, i) for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)]
load_shelve_times = [load_speed_test(load_by_shelve, i) for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)]
save_cpickle_times = [save_speed_test(save_by_cpickle, i) for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)]
load_cpickle_times = [load_speed_test(load_by_cpickle, i) for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)]
save_sqlite_times = [save_speed_test(save_by_sqlite, i) for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)]
load_sqlite_times = [load_speed_test(load_by_sqlite, i) for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)]
# plot save time
plt.plot([i for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)], save_pickle_times, "xr-", label="pickle")
plt.plot([i for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)], save_npy_times, "xg-", label="numpy")
plt.plot([i for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)], save_h5py_times, "xb-", label="h5py")
plt.plot([i for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)], save_shelve_times, "xc-", label="shelve")
plt.plot([i for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)], save_cpickle_times, "xm-", label="cpickle")
plt.plot([i for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)], save_sqlite_times, "xy-", label="sqlite")
plt.legend(loc='upper left')
plt.title("Save Time")
plt.xlabel("numpy dimension")
plt.ylabel("time(second)")
plt.savefig("graph/save_time.png")
plt.show()

# plot load time
plt.plot([i for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)], load_pickle_times, "xr-", label="pickle")
plt.plot([i for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)], load_npy_times, "xg-", label="numpy")
plt.plot([i for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)], load_h5py_times, "xb-", label="h5py")
plt.plot([i for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)], load_shelve_times, "xc-", label="shelve")
plt.plot([i for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)], load_cpickle_times, "xm-", label="cpickle")
plt.plot([i for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)], load_sqlite_times, "xy-", label="sqlite")
plt.legend(loc='upper left')
plt.title("Load Time")
plt.xlabel("numpy dimension")
plt.ylabel("time(second)")
plt.savefig("graph/load_time.png")
plt.show()

# plot save time
# plt.plot([i for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)], save_pickle_times, "xr-", label="pickle")
plt.plot([i for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)], save_npy_times, "xg-", label="numpy")
plt.plot([i for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)], save_h5py_times, "xb-", label="h5py")
# plt.plot([i for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)], save_shelve_times, "xc-", label="shelve")
# plt.plot([i for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)], save_cpickle_times, "xm-", label="cpickle")
plt.plot([i for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)], save_sqlite_times, "xy-", label="sqlite")
plt.legend(loc='upper left')
plt.title("Save Time")
plt.xlabel("numpy dimension")
plt.ylabel("time(second)")
plt.savefig("graph/save_time2.png")
plt.show()

# plot load time
# plt.plot([i for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)], load_pickle_times, "xr-", label="pickle")
plt.plot([i for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)], load_npy_times, "xg-", label="numpy")
plt.plot([i for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)], load_h5py_times, "xb-", label="h5py")
# plt.plot([i for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)], load_shelve_times, "xc-", label="shelve")
# plt.plot([i for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)], load_cpickle_times, "xm-", label="cpickle")
plt.plot([i for i in xrange(min_mat_size, max_mat_size + 1, step_mat_size)], load_sqlite_times, "xy-", label="sqlite")
plt.legend(loc='upper left')
plt.title("Load Time")
plt.xlabel("numpy dimension")
plt.ylabel("time(second)")
plt.savefig("graph/load_time2.png")
plt.show()
