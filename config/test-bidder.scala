
import com.twitter.logging.config.ConsoleHandlerConfig
import com.twitter.logging.{LoggerFactory, Level}
import com.twitter.parrot.config.ParrotLauncherConfig

new ParrotLauncherConfig {
    localMode = true
    jobName = "testbidder"
    port = 9000
    victims = "localhost"
    log = "config/replay.log"
//    requestRate = 5
//    maxRequests = 20
    timeUnit = "MINUTES"
    reuseFile = false
    scheme = "http"
    parser = "http"
    verboseCmd = true
    doConfirm = false

    loggers = new LoggerFactory(
        level = Level.DEBUG,
        handlers = new ConsoleHandlerConfig()
    )

    //TODO: can't get this to work
    /*
    imports =
            """import org.jboss.netty.handler.codec.http.HttpResponse
            import me.sonar.adx.openrtb.BidLoadTest
            """

    loadTest = "new BidLoadTest(service.get)"
    */
}


