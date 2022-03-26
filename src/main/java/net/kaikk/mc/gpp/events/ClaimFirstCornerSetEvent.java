package net.kaikk.mc.gpp.events;

import org.bukkit.entity.Player;

/** Called when a player starts to create a new claim (sets its first corner) */
public class ClaimFirstCornerSetEvent extends ClaimEvent { //This should not extend ClaimEvent... but whatever
	public ClaimFirstCornerSetEvent(Player player) {
		super(null, player);
	}
}
