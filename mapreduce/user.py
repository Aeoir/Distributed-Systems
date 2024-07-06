import multiprocessing
import os
import random

INFINTE_VAL = 9999999
MIN_C = 0
MAX_C = 10

def master(num_m, num_r, num_k, num_iter):
    
    point_list = []
    f = open('Data/Input/points.txt', 'r')

    for line in f:
        point_list.append(line)
    f.close()    
    
    file_list = []
    for i in range(num_m):
        os.makedirs(f"Mappers/M{i}")
        file_list.append(open(f"split_{i}", 'w'))
        
    for i, j in enumerate(point_list):
        file_list[(i%num_m)].write(j)    
        
    print("Input Split Created")  
    
def mapper(num, num_r):
    #RPC the split num
    list_centroid = []
    
    f = open('Reducers/centroids.txt', 'r')
    for line in f:
        list_centroid.append(line.strip())
    f.close()
    
    split_file = f'split_{num}'
    f = open(split_file, 'r')
    intermediate_output = []    

    for line in f:
        
        min_d = INFINTE_VAL
        out_c = None
        point = line.strip().split(',')
        x = eval(point[0])
        y = eval(point[1])
        
        for i, centroid in enumerate(list_centroid):
            point = centroid.split(',')
            c_x = eval(point[0])
            c_y = eval(point[1])
            temp_d = (x-c_x)**2 + (y-c_y)**2

            if temp_d<min_d:
                min_d = temp_d
                out_c = i
                
        intermediate_output.append(f'{out_c},{line.strip()}\n')
    f.close()
                    
    file_list = []
    for i in range(num_r):
        file_list.append(open(f"Mappers/M{num}/partition_{i}", 'w'))

    for i, j in enumerate(intermediate_output):
        key = eval(j.split(',')[0])
        file_list[(key%num_r)].write(j)

def reducer(num, num_m):

    # Shuffle and Sort
    all_val_list = []
    for i in range(num_m):
        f = open(f"Mappers/M{i}/partition_{num}")
        for line in f:
            all_val_list.append(line.strip())

    keys_dict = {}
    for i in all_val_list:
        temp = i.split(',')
        if temp[0] in keys_dict:
            keys_dict[temp[0]].append(temp)
        else:
            keys_dict[temp[0]] = [temp]
            
    # Reducing
    centroid_dict = {}
    
    for keys in keys_dict:
        c_x = 0
        c_y = 0
        temp = keys_dict[keys]
        for i in temp:
            x = eval(i[1])
            y = eval(i[2])
            c_x += x
            c_y += y
        centroid_dict[keys] = f"{c_x/len(temp)},{c_y/len(temp)}"
    
    f = open(f'Reducers/R{num}', 'w')
    for key in centroid_dict:
        f.write(f"{key},{centroid_dict[key]}")
        f.write('\n')
    
if __name__ == "__main__":
    
    print("Welcome to Kmeans by MapReduce")

    num_m = int(input("Number of Mappers: "))
    num_r = int(input("Number of Reducers: "))
    num_k = int(input("Number of Centroids: "))
    num_iter = int(input("Number of Iterations: "))
    
    f = open('Reducers/centroids.txt', 'w')
    for i in range(num_k):
        x = random.uniform(MIN_C,MAX_C)
        y = random.uniform(MIN_C,MAX_C)
        f.write(f"{x},{y}\n")
    f.close()

    master_p = multiprocessing.Process(target=master, args=(num_m, num_r, num_k, num_iter))
    master_p.start()
    master_p.join()
    
    for iter in range(num_iter):
        mapper_list = []

        for i in range(num_m):
            p = multiprocessing.Process(target=mapper, args=(i,num_r))
            mapper_list.append(p)
            p.start()

        for p in mapper_list:
            p.join()

        print("All mappers have returned", iter) #Should be in Master
        
        reducer_list = []
        
        for i in range(num_r):
            p = multiprocessing.Process(target=reducer, args=(i,num_m))
            reducer_list.append(p)
            p.start()

        for p in reducer_list:
            p.join()

        print("All reducers have returned", iter) #Should be in Master

        list_centroid = []
        
        f = open('Reducers/centroids.txt', 'r')
        for line in f:
            list_centroid.append(line.strip())  
            
        f.close()
        print(list_centroid)
        
        for i in range(num_r):
            f = open(f'Reducers/R{i}', 'r')
            for line in f:
                temp = line.strip().split(',')
                list_centroid[int(temp[0])] = f"{temp[1]},{temp[2]}"
            f.close()
                
        print(list_centroid)
        f = open('Reducers/centroids.txt', 'w')
        for line in list_centroid:
            f.write(line+'\n')
        f.close()
    
    input()
    os.system("rm -r -f M*")