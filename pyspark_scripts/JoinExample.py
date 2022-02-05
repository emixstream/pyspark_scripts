
import collections
Rel_A=collections.namedtuple('Rel_A', 'k a1 a2')
Rel_B=collections.namedtuple('Rel_B', 'k b1 b2')


rel_a1=Rel_A("k1",1,2)
rel_a2=Rel_A("k2",3,4)
rel_a3=Rel_A("k3",5,6)
rel_b1=Rel_B("k1",7,8)
rel_b2=Rel_B("k2",9,10)
rel_b3=Rel_B("k3",11,12)






rel_a_rdd=sc.parallelize([rel_a1,rel_a2,rel_a3])
rel_b_rdd=sc.parallelize([rel_b1,rel_b2,rel_b3])



rel_a_rdd.collect()






rel_b_rdd.collect()





rel_c_rdd=rel_a_rdd.join(rel_b_rdd)




rel_c_rdd.collect()





rel_join_rdd=rel_a_rdd.map(lambda x: (x.k, (x.a1,x.a2))).join(rel_b_rdd.map(lambda x: (x.k,(x.b1,x.b2))))



rel_join_rdd.collect()






t1=rel_join_rdd.take(1)





type(t1)








t1









t1[0]








t1[0][1]






t1[0][1][1][1]


