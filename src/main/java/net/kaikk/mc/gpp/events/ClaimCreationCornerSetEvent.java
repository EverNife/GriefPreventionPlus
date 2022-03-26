package net.kaikk.mc.gpp.events;

import org.bukkit.Location;
import org.bukkit.entity.Player;

/** Called when a player starts to create a new claim (sets its first corner) */
public class ClaimCreationCornerSetEvent extends ClaimEvent { //This should not extend ClaimEvent... but whatever
	private final Location corner;
	private final boolean firstCorner;

	public ClaimCreationCornerSetEvent(Player player, Location corner, boolean firstCorner) {
		super(null, player);
		this.corner = corner;
		this.firstCorner = firstCorner;
	}

	public Location getCorner() {
		return corner;
	}

	public boolean isFirstCorner(){
		return firstCorner == true;
	}

	public boolean isSecondCorner(){
		return firstCorner == false;
	}
}
