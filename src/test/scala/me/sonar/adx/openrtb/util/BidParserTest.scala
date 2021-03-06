package me.sonar.adx.openrtb.util

import org.scalatest.FlatSpec
import com.twitter.util.{Throw, Return}
import com.twitter.util.Throw

import com.twitter.util.Return
import com.codahale.jerkson.SonarJson
import SonarJson._
import com.twitter.util.Throw
import com.twitter.util.Return
import org.openrtb.BidRequest
import java.io.StringWriter
import com.sonar.expedition.common.serialization.Serialization._

class BidParserTest extends FlatSpec {

    "BidParser" should "apply" in {
        val lines = Seq[String](
            "R,OK,2013-02-08 05:25:45.899,50000,apple_iphone,356594783591455944,88fea1636d91ed254a24bd7d85904afe6407d6ed,338c4e334fc8ba251b0eb36690fe50ce81c53c8b,Utilities,CN,US,iPhone OS,33813,LAKELAND,IPHONE_APPLICATION,192.168.1.6,",
            "R,OK,2013-02-08 05:25:45.892,0,samsung_sgh-t999,356594783589883074,28de0180777ecac44cbf4ce2459400309d8aaaac,67e8ec07ebc2b161ab8e167cdadb9706a1808deb,Social Networking,CN,US,Android,77318,WILLIS,ANDROID_APPLICATION,173.204.162.111,30.424928%2c-95.479942",
            "R,NO_AD_MATCHING,2013-02-08 05:25:45.911,,apple_iphone,356594783594601684,043118abc4307ef86915b35d11028c60498ff8f1,510a69c7918eae1cb61aae47214f86d7b4f307a2,Automotive,CN,,iPhone OS,,,IPHONE_APPLICATION,107.22.42.26,",
            "R,ADSPOT_NOT_FOUND,2013-02-08 05:25:45.999,,apple_ipad,356594783617408296,84ee868c58e4aa9d3a0d6e0d4d4a0fbce63d5d8e,4c04398e8efe2c278324ce062ad19eb0c8a4b6b6,,CN,US,iPhone OS,18974,WARMINSTER,,173.204.162.79,40.206775%2c-75.099616"
        )

        lines foreach {
            line =>
                BidParser(line) match {
                    case Return(bidRequest) =>
                        val writer = new StringWriter
                        generate[BidRequest](bidRequest, writer)
                        val body = writer.toString
                        println(body)
                        val br = fromByteArray(body.getBytes, new BidRequest)
                        None
                    case Throw(t) =>
                        assert(false)
                        None
                }
        }
    }
}