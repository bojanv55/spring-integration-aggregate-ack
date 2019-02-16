package me.vukas;

import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class AckingState {
    private final org.slf4j.Logger logger = LoggerFactory.getLogger(AckingState.class);
    private Map<Integer, Set<Long>> channelToTag = new ConcurrentHashMap<>();

    public void addMessage(Integer channel, Long tag){
        channelToTag.compute(channel, (k,prevV) -> {
            if(prevV!=null){
                if(channelToTag.get(channel).contains(tag)) {
                    logger.warn("Trying to ADD message that already exists (not ACKED) on " + channel + " old tag (non acked) " + prevV + " new tag " + tag);
                }
                prevV.add(tag);
                return prevV;
            }
            Set<Long> s = new HashSet<>();
            s.add(tag);
            return s;
        });
    }

    public void removeMessage(Integer channel, Long tag){
        channelToTag.compute(channel, (k,prevV) -> {
            if(prevV==null){
                logger.warn("Trying to REMOVE NONEXISTING message on channel " + channel + " with tag " + tag);
                return null;
            }
            if(!prevV.contains(tag)){
                logger.warn("Trying to REMOVE NONEXISTING message on channel " + channel + " with tag " + tag);
            }
            prevV.remove(tag);
            return prevV;
        });
    }

    public int getUnacked(){
        AtomicInteger unacked = new AtomicInteger(0);
        channelToTag.entrySet().stream()
                .forEach(e -> {
                    if(!e.getValue().isEmpty()){
                        unacked.addAndGet(e.getValue().size());
                    }
                });
        return unacked.get();
    }

    public void report(){
        logger.info("TOTAL NUMBER OF CHANNELS USED IS " + channelToTag.size());
        logger.info("TOTAL NUMBER OF UNACKED ELEMENTS IS " + getUnacked());
    }
}
