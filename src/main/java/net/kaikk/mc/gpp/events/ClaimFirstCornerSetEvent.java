package net.kaikk.mc.gpp.events;

import org.bukkit.Location;
import org.bukkit.entity.Player;

/** Called when a player starts to create a new claim (sets its first corner) */
public class ClaimFirstCornerSetEvent extends ClaimEvent { //This should not extend ClaimEvent... but whatever
	private final Location firstCorner;
	public ClaimFirstCornerSetEvent(Player player, Location firstCorner) {
		super(null, player);
		this.firstCorner = firstCorner;
	}

	public Location getFirstCorner() {
		return firstCorner;
	}
}
