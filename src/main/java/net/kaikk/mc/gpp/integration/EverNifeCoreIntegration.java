package net.kaikk.mc.gpp.integration;

import br.com.finalcraft.evernifecore.util.FCBukkitUtil;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;

public class EverNifeCoreIntegration {

    private static Boolean isPresent;

    public static boolean isPresent() {
        if (isPresent == null) {
            isPresent = Bukkit.getPluginManager().isPluginEnabled("EverNifeCore");
        }
        return isPresent;
    }

    public static boolean isFakePlayer(Player player){
        return isPresent() && FCBukkitUtil.isFakePlayer(player);
    }

}
