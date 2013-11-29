///* Copyright 2013 Future TV, Inc.
// *
// *      Licensed under the Apache License, Version 2.0 (the "License");
// *      you may not use this file except in compliance with the License.
// *      You may obtain a copy of the License at
// *
// *          http://www.apache.org/licenses/LICENSE-2.0
// *
// *      Unless required by applicable law or agreed to in writing, software
// *      distributed under the License is distributed on an "AS IS" BASIS,
// *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *      See the License for the specific language governing permissions and
// *      limitations under the License.
// */
//
//import org.apache.mahout.cf.taste.common.TasteException;
//import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
//import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
//import org.apache.mahout.cf.taste.impl.recommender.BiasedItemBasedRecommender;
//import org.apache.mahout.cf.taste.impl.recommender.CachingRecommender;
//import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
//import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
//import org.apache.mahout.cf.taste.impl.recommender.knn.KnnItemBasedRecommender;
//import org.apache.mahout.cf.taste.impl.recommender.slopeone.SlopeOneRecommender;
//import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
//import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
//import org.apache.mahout.cf.taste.impl.similarity.TanimotoCoefficientSimilarity;
//import org.apache.mahout.cf.taste.impl.similarity.UncenteredCosineSimilarity;
//import org.apache.mahout.cf.taste.model.DataModel;
//import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
//import org.apache.mahout.cf.taste.recommender.RecommendedItem;
//import org.apache.mahout.cf.taste.recommender.Recommender;
//import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
//import org.apache.mahout.cf.taste.similarity.UserSimilarity;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.List;
//
///**
// * Created with IntelliJ IDEA.
// * User: lei
// * Date: 13-10-18
// * Time: 上午11:04
// * To change this template use File | Settings | File Templates.
// */
//public class Test {
//    public static void main(String [] args) throws IOException, TasteException {
//        DataModel model=new FileDataModel(new File("D:\\test.txt"));
//        ItemSimilarity item = new UncenteredCosineSimilarity(model);
//        Recommender recommender= new GenericItemBasedRecommender(model,item);
//        List<RecommendedItem> list= recommender.recommend(1,4);
////        DataModel model = new FileDataModel(new File("/home/huhui/movie_preferences.txt"));//构造数据模型
////        Recommender recommender = new CachingRecommender(new SlopeOneRecommender(model));//构造推荐引擎
////        UserSimilarity similarity = new PearsonCorrelationSimilarity(model);//用PearsonCorrelation 算法计算用户相似度
////        UserNeighborhood neighborhood = new NearestNUserNeighborhood(200, similarity, model);//计算用户的“邻居”，这里将与该用户最近距离为 3 的用户设置为该用户的“邻居”。
////        Recommender recommender = new CachingRecommender(new GenericUserBasedRecommender(model, neighborhood, similarity));//构造推荐引擎，采
////        List<RecommendedItem>list = recommender.recommend(847, 10);//得到推荐结果
//        for(RecommendedItem it: list){
//            System.out.println(it.getItemID()+"--"+it.getValue());
//
//        }
//        System.out.println(list.size());
//    }
//}
