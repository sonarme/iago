
import com.twitter.logging.config.ConsoleHandlerConfig
import com.twitter.logging.{LoggerFactory, Level}
import com.twitter.parrot.config.ParrotLauncherConfig

new ParrotLauncherConfig {
    localMode = true
    jobName = "testbidder"
    port = 80
    victims = "rtb.sonar.me"
    log = "/media/ephemeral0/jumptap/Jumptap_all_parts.txt"
//    log = "config/replay.log"
    requestRate = 100
    maxRequests = 5000
    timeUnit = "MINUTES"
    reuseFile = false
    scheme = "http"
    parser = "http"
//    verboseCmd = true
    doConfirm = false

    loggers = new LoggerFactory(
        level = Level.DEBUG,
        handlers = new ConsoleHandlerConfig()
    )
    /*
    imports =
            """import org.jboss.netty.handler.codec.http.HttpResponse
            import me.sonar.adx.openrtb.BidRecordProcessor
            """

    loadTest = "new BidRecordProcessor(service.get)"
    */
}


